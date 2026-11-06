package lengthrouting

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	videoMaxBytes   = int64(100 * 1024 * 1024)
	videoMaxSeconds = 600
	videoMaxFrames  = 1280
	videoTimeout    = 120 * time.Second
)

var (
	videoDownloadSlots = make(chan struct{}, 32)
	videoDecodeSlots   = make(chan struct{}, 4)
	errVideoSource     = errors.New("video source is unavailable or disallowed")
	errVideoLimit      = errors.New("video exceeds processing limits")
	errVideoDecode     = errors.New("video decoding failed")
)

// 请求内共用一个超时和结果缓存；只在实际遇到视频时由计数入口创建。
func newVideoFrameCounter(parent context.Context) (func(string) (int, error), context.CancelFunc) {
	return makeVideoFrameCounter(parent, videoTimeout, countVideoFrames)
}

func makeVideoFrameCounter(parent context.Context, timeout time.Duration, process func(context.Context, string) (int, error)) (func(string) (int, error), context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	type result struct {
		frames int
		err    error
	}
	cache := make(map[[32]byte]result)
	var mu sync.Mutex
	return func(raw string) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		key := sha256.Sum256([]byte(raw))
		if found, ok := cache[key]; ok {
			return found.frames, found.err
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		frames, err := process(ctx, raw)
		cache[key] = result{frames, err}
		return frames, err
	}, cancel
}

func countVideoFrames(ctx context.Context, raw string) (int, error) {
	// 没有工具时不先下载；调用方保留多模态并将长度标成 unknown。
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return 0, errVideoDecode
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		return 0, errVideoDecode
	}
	select {
	case videoDownloadSlots <- struct{}{}:
	default:
		return 0, errVideoLimit
	}
	file, cleanup, err := openVideoSource(ctx, raw, videoHTTPClient, videoMaxBytes)
	<-videoDownloadSlots
	if err != nil {
		return 0, err
	}
	defer cleanup()
	select {
	case videoDecodeSlots <- struct{}{}:
	default:
		return 0, errVideoLimit
	}
	defer func() { <-videoDecodeSlots }()
	return decodeVideoFrames(ctx, ffmpeg, ffprobe, file)
}

// 仅拨号到已经校验过的解析结果；不经环境代理，重定向也必须重新走相同校验。
var videoHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 nil,
		DialContext:           dialPublicVideo,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		MaxIdleConns:          16,
		IdleConnTimeout:       30 * time.Second,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errVideoSource
		}
		return validateVideoURL(req.URL)
	},
}

var blockedVideoNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001::/32"), netip.MustParsePrefix("2001:2::/48"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"),
}

func publicVideoIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range blockedVideoNetworks {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func validateVideoURL(u *url.URL) error {
	if u == nil || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Opaque != "" || u.Hostname() == "" {
		return errVideoSource
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") || strings.Contains(host, "%") {
		return errVideoSource
	}
	if ip := net.ParseIP(host); ip != nil && !publicVideoIP(ip) {
		return errVideoSource
	}
	return nil
}

func dialPublicVideo(ctx context.Context, network, address string) (net.Conn, error) {
	return dialCheckedVideo(ctx, network, address, net.DefaultResolver.LookupIPAddr, (&net.Dialer{Timeout: 10 * time.Second}).DialContext)
}

func dialCheckedVideo(ctx context.Context, network, address string, resolve func(context.Context, string) ([]net.IPAddr, error), dial func(context.Context, string, string) (net.Conn, error)) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, errVideoSource
	}
	ips, err := resolve(ctx, host)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errVideoSource
	}
	if len(ips) == 0 {
		return nil, errVideoSource
	}
	// 混有内网地址的解析也拒绝；拨号固定使用本次已验 IP，避免二次 DNS 解析绕过。
	for _, ip := range ips {
		if ip.Zone != "" || !publicVideoIP(ip.IP) {
			return nil, errVideoSource
		}
	}
	for _, ip := range ips {
		conn, err := dial(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, errVideoSource
}

func videoExtension(raw, contentType string) string {
	switch strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0])) {
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "video/webm":
		return ".webm"
	case "video/x-matroska":
		return ".mkv"
	case "video/x-msvideo":
		return ".avi"
	}
	if parsed, err := url.Parse(raw); err == nil {
		switch ext := strings.ToLower(path.Ext(parsed.Path)); ext {
		case ".mp4", ".mov", ".webm", ".mkv", ".avi":
			return ext
		}
	}
	return ""
}

func openVideoSource(ctx context.Context, raw string, client *http.Client, maxBytes int64) (string, func(), error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	var source io.Reader
	var ext string
	if strings.HasPrefix(raw, "data:") {
		comma := strings.IndexByte(raw, ',')
		if comma < 0 || comma > 128 {
			return "", nil, errVideoSource
		}
		metadata := strings.Split(raw[5:comma], ";")
		if len(metadata) < 2 || !strings.EqualFold(metadata[len(metadata)-1], "base64") {
			return "", nil, errVideoSource
		}
		ext = videoExtension("", metadata[0])
		payload := raw[comma+1:]
		if int64(len(payload)) > (maxBytes+2)/3*4+4 {
			return "", nil, errVideoLimit
		}
		var encoded io.Reader = strings.NewReader(payload)
		if padding := len(payload) % 4; padding != 0 {
			encoded = io.MultiReader(encoded, strings.NewReader(strings.Repeat("=", 4-padding)))
		}
		source = base64.NewDecoder(base64.StdEncoding, encoded)
	} else {
		parsed, err := url.Parse(raw)
		if err != nil || validateVideoURL(parsed) != nil {
			return "", nil, errVideoSource
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		if err != nil {
			return "", nil, errVideoSource
		}
		response, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return "", nil, ctx.Err()
			}
			return "", nil, errVideoSource
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return "", nil, errVideoSource
		}
		if response.ContentLength > maxBytes {
			return "", nil, errVideoLimit
		}
		ext = videoExtension(raw, response.Header.Get("Content-Type"))
		source = response.Body
	}
	if ext == "" {
		return "", nil, errVideoSource
	}
	file, err := os.CreateTemp("", "oneapi-length-video-*"+ext)
	if err != nil {
		return "", nil, errVideoSource
	}
	cleanup := func() { _ = os.Remove(file.Name()) }
	size, copyErr := io.Copy(file, io.LimitReader(&videoContextReader{ctx, source}, maxBytes+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || size > maxBytes || size == 0 {
		cleanup()
		if ctx.Err() != nil {
			return "", nil, ctx.Err()
		}
		if size > maxBytes {
			return "", nil, errVideoLimit
		}
		return "", nil, errVideoSource
	}
	return file.Name(), cleanup, nil
}

type videoContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *videoContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

type videoOutput struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (w *videoOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > w.limit-w.buffer.Len() {
		p = p[:w.limit-w.buffer.Len()]
		w.overflow = true
	}
	_, _ = w.buffer.Write(p)
	return n, nil
}

func runVideoCommand(ctx context.Context, program string, args ...string) ([]byte, error) {
	output := &videoOutput{limit: 256 * 1024}
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Stdout = output
	// 不把解码器输出或文件地址带入请求错误；同时限制工具输出在内存中的占用。
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errVideoDecode
	}
	if output.overflow {
		return nil, errVideoLimit
	}
	return output.buffer.Bytes(), nil
}

func decodeVideoFrames(ctx context.Context, ffmpeg, ffprobe, file string) (int, error) {
	// 格式白名单排除播放列表；解码器只能打开落盘文件，不能发起额外网络访问。
	// 限制单次分配为 256 MiB，并限制解码像素；这不是对工具总 RSS 的保证。
	inputs := []string{"-max_alloc", "268435456", "-max_pixels", "35389440", "-protocol_whitelist", "file,pipe", "-format_whitelist", "mov,matroska,webm,avi"}
	args := append([]string{"-v", "error"}, inputs...)
	args = append(args, "-select_streams", "v:0", "-show_entries", "format=duration:stream=width,height", "-of", "json", file)
	probe, err := runVideoCommand(ctx, ffprobe, args...)
	if err != nil {
		return 0, err
	}
	var metadata struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Width  int64 `json:"width"`
			Height int64 `json:"height"`
		} `json:"streams"`
	}
	if json.Unmarshal(probe, &metadata) != nil || len(metadata.Streams) == 0 {
		return 0, errVideoDecode
	}
	seconds, err := strconv.ParseFloat(metadata.Format.Duration, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 {
		return 0, errVideoDecode
	}
	if seconds > videoMaxSeconds {
		return 0, errVideoLimit
	}
	dimensions := metadata.Streams[0]
	// 允许常见 8K 视频；过大的单帧在完整解码前拒绝，避免小文件引起大内存分配。
	if dimensions.Width <= 0 || dimensions.Height <= 0 || dimensions.Width > 8192 || dimensions.Height > 8192 || dimensions.Width*dimensions.Height > 8192*4320 {
		return 0, errVideoLimit
	}
	args = append([]string{"-nostdin", "-v", "error", "-threads", "1"}, inputs...)
	args = append(args, "-i", file, "-map", "0:v:0", "-an", "-sn", "-dn", "-vf", "fps=2", "-frames:v", strconv.Itoa(videoMaxFrames), "-t", strconv.Itoa(videoMaxSeconds+1), "-progress", "pipe:1", "-nostats", "-f", "null", "-")
	output, err := runVideoCommand(ctx, ffmpeg, args...)
	if err != nil {
		return 0, err
	}
	frames := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "frame=") {
			parsed, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "frame=")))
			if err != nil || parsed < 0 || parsed > videoMaxFrames {
				return 0, errVideoDecode
			}
			frames = parsed
		}
	}
	// 多解码至多一秒用于识别低报时长，不能把截断的视频静默当成完整输入。
	if frames > videoMaxSeconds*2 {
		return 0, errVideoLimit
	}
	if frames <= 0 {
		return 0, errVideoDecode
	}
	return frames, nil
}
