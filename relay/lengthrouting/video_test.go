package lengthrouting

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVideoURLPolicy(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1/a.mp4", "http://[::1]/a", "http://[::ffff:127.0.0.1]/a", "http://10.0.0.1/a", "http://172.31.1.1/a", "http://192.168.1.1/a", "http://169.254.169.254/a", "http://100.64.0.1/a", "http://0.0.0.0/a", "http://224.0.0.1/a", "http://[fc00::1]/a", "http://[fe80::1%25eth0]/a", "http://[64:ff9b::7f00:1]/a", "http://localhost/a", "http://a.localhost./a", "http://machine.local/a", "http://user:pass@example.com/a", "file:///tmp/a.mp4", "ftp://example.com/a", "http:///a", "http://192.0.2.1/a", "http://[2001:db8::1]/a"} {
		t.Run(raw, func(t *testing.T) {
			parsed, err := url.Parse(raw)
			if err == nil && validateVideoURL(parsed) == nil {
				t.Fatalf("accepted unsafe URL %q", raw)
			}
		})
	}
	for _, raw := range []string{"https://example.com/a.mp4?token=x", "http://8.8.8.8/video", "https://[2606:4700:4700::1111]/video"} {
		parsed, err := url.Parse(raw)
		if err != nil || validateVideoURL(parsed) != nil {
			t.Fatalf("rejected public URL %q", raw)
		}
	}
}

func TestVideoDialPinsValidatedAddresses(t *testing.T) {
	var got string
	resolve := func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}
	dial := func(_ context.Context, _ string, address string) (net.Conn, error) {
		got = address
		a, b := net.Pipe()
		_ = b.Close()
		return a, nil
	}
	conn, err := dialCheckedVideo(context.Background(), "tcp", "public.example:443", resolve, dial)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if got != "8.8.8.8:443" {
		t.Fatalf("dialed unresolved host: %q", got)
	}
	for _, ips := range [][]net.IPAddr{nil, {{IP: net.ParseIP("127.0.0.1")}}, {{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.1.1.1")}}, {{IP: net.ParseIP("8.8.8.8"), Zone: "eth0"}}} {
		got = ""
		_, err := dialCheckedVideo(context.Background(), "tcp", "public.example:443", func(context.Context, string) ([]net.IPAddr, error) { return ips, nil }, dial)
		if err == nil || got != "" {
			t.Fatalf("unsafe resolver result was dialed: %v %q", ips, got)
		}
	}
}

type videoRoundTrip func(*http.Request) (*http.Response, error)

func (f videoRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestVideoRedirectRevalidatesBeforeTransport(t *testing.T) {
	for _, location := range []string{"http://127.0.0.1/private.mp4", "http://user:secret@example.com/a.mp4", "file:///tmp/private.mp4"} {
		calls := 0
		client := &http.Client{CheckRedirect: videoHTTPClient.CheckRedirect, Transport: videoRoundTrip(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": []string{location}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		})}
		_, _, err := openVideoSource(context.Background(), "https://public.example/video.mp4", client, 32)
		if err == nil || calls != 1 {
			t.Fatalf("redirect reached transport: calls=%d err=%v", calls, err)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com/video.mp4", nil)
	if videoHTTPClient.CheckRedirect(req, make([]*http.Request, 5)) == nil {
		t.Fatal("unbounded redirects")
	}
}

func TestVideoInlineSizeFormatAndCleanup(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	valid := "data:video/mp4;base64," + base64.StdEncoding.EncodeToString([]byte("abcd"))
	for _, raw := range []string{valid, strings.TrimRight(valid, "=")} {
		name, cleanup, err := openVideoSource(context.Background(), raw, nil, 4)
		if err != nil {
			t.Fatal(err)
		}
		content, err := os.ReadFile(name)
		if err != nil || string(content) != "abcd" {
			t.Fatalf("unexpected decoded file: %q %v", content, err)
		}
		cleanup()
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatal("temporary source not removed")
		}
	}
	for _, raw := range []string{valid, "data:video/mp4;base64,@@@@", "data:video/mp4;base64,", "data:text/plain;base64,YWJj", "data:video/mp4,abcd", "data:video/mp4;base64", "data:video/mp4;base64," + strings.Repeat("A", 100)} {
		_, _, err := openVideoSource(context.Background(), raw, nil, 3)
		if err == nil {
			t.Fatalf("accepted invalid/oversized inline source %q", raw)
		}
	}
	files, err := os.ReadDir(temp)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary sources leaked: %v %v", files, err)
	}
}

func TestVideoHTTPReadBoundAndCleanup(t *testing.T) {
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
		length                  int64
		want                    bool
	}{
		{"valid", "abcd", "video/mp4", 200, 4, true},
		{"extension fallback", "abcd", "application/octet-stream", 200, -1, true},
		{"declared oversize", "abcd", "video/mp4", 200, 99, false},
		{"chunked oversize", "abcde", "video/mp4", 200, -1, false},
		{"empty", "", "video/mp4", 200, 0, false},
		{"status failure", "abcd", "video/mp4", 503, 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: videoRoundTrip(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, ContentLength: tc.length, Header: http.Header{"Content-Type": []string{tc.contentType}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})}
			name, cleanup, err := openVideoSource(context.Background(), "https://public.example/a.mp4", client, 4)
			if tc.want {
				if err != nil {
					t.Fatal(err)
				}
				content, _ := os.ReadFile(name)
				if string(content) != tc.body {
					t.Fatal("source changed")
				}
				cleanup()
			} else if err == nil {
				cleanup()
				t.Fatal("expected failure")
			}
		})
	}
	files, _ := os.ReadDir(temp)
	if len(files) != 0 {
		t.Fatalf("temporary sources leaked: %v", files)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := openVideoSource(ctx, "data:video/mp4;base64,YWJj", nil, 8)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}

func TestVideoCounterCachesFailuresAndSharesBudget(t *testing.T) {
	var calls int
	failed := errors.New("test failure")
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	var deadline time.Time
	counter, cancel := makeVideoFrameCounter(parent, time.Second, func(ctx context.Context, raw string) (int, error) {
		calls++
		current, ok := ctx.Deadline()
		if !ok {
			t.Fatal("no shared deadline")
		}
		if deadline.IsZero() {
			deadline = current
		} else if !deadline.Equal(current) {
			t.Fatal("deadline reset between videos")
		}
		if raw == "bad" {
			return 0, failed
		}
		return 4, nil
	})
	defer cancel()
	for i := 0; i < 2; i++ {
		frames, err := counter("bad")
		if frames != 0 || !errors.Is(err, failed) {
			t.Fatal("failure changed")
		}
	}
	for i := 0; i < 2; i++ {
		frames, err := counter("good")
		if frames != 4 || err != nil {
			t.Fatal("success changed")
		}
	}
	if calls != 2 {
		t.Fatalf("processed duplicate URLs: %d", calls)
	}
	cancelParent()
	if _, err := counter("new"); !errors.Is(err, context.Canceled) {
		t.Fatalf("new URL ignored canceled shared budget: %v", err)
	}
	if calls != 2 {
		t.Fatal("processed after cancellation")
	}
}

func TestVideoCounterConcurrentDeduplication(t *testing.T) {
	var calls int32
	counter, cancel := makeVideoFrameCounter(nil, time.Second, func(context.Context, string) (int, error) { atomic.AddInt32(&calls, 1); return 3, nil })
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n, err := counter("same"); n != 3 || err != nil {
				t.Errorf("count=%d err=%v", n, err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("duplicate processing: %d", calls)
	}
}

func videoExecutable(t *testing.T, name, body string) string {
	t.Helper()
	filename := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(filename, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return filename
}

func TestVideoDecodeLimitsAndProgress(t *testing.T) {
	for _, tc := range []struct {
		name, duration, dimensions, progress string
		want                                 int
		limit                                bool
	}{
		{"actual count", "2", `"width":16,"height":16`, "frame=1\nframe=4\nprogress=end", 4, false},
		{"duration boundary", "600", `"width":16,"height":16`, "frame=1200\nprogress=end", 1200, false},
		{"understated duration", "2", `"width":16,"height":16`, "frame=1201\nprogress=end", 0, true},
		{"duration exceeded", "600.1", `"width":16,"height":16`, "frame=4", 0, true},
		{"unbounded duration", "NaN", `"width":16,"height":16`, "frame=4", 0, false},
		{"oversized frame", "2", `"width":8193,"height":16`, "frame=4", 0, true},
		{"no frames", "2", `"width":16,"height":16`, "frame=0\nprogress=end", 0, false},
		{"invalid progress", "2", `"width":16,"height":16`, "frame=no", 0, false},
		{"frames cap", "2", `"width":16,"height":16`, "frame=1281", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := videoExecutable(t, "ffprobe", "printf '%s' '{\"format\":{\"duration\":\""+tc.duration+"\"},\"streams\":[{"+tc.dimensions+"}]}'")
			ffmpeg := videoExecutable(t, "ffmpeg", "printf '%s' '"+tc.progress+"'")
			frames, err := decodeVideoFrames(context.Background(), ffmpeg, probe, "/unused/test.mp4")
			if tc.want > 0 {
				if err != nil || frames != tc.want {
					t.Fatalf("frames=%d err=%v", frames, err)
				}
			} else if err == nil {
				t.Fatal("invalid decoder output accepted")
			}
			if tc.limit && !errors.Is(err, errVideoLimit) {
				t.Fatalf("limit classification lost: %v", err)
			}
		})
	}
}

func TestVideoCommandTimeoutAndOutputLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	slow := videoExecutable(t, "slow", "exec /bin/sleep 5")
	if _, err := runVideoCommand(ctx, slow); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost timeout: %v", err)
	}
	flood := videoExecutable(t, "flood", "exec /usr/bin/head -c 300000 /dev/zero")
	if _, err := runVideoCommand(context.Background(), flood); !errors.Is(err, errVideoLimit) {
		t.Fatalf("unbounded tool output: %v", err)
	}
}

func TestVideoMissingToolsDoesNotDownload(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, err := countVideoFrames(context.Background(), "https://public.example/a.mp4"); !errors.Is(err, errVideoDecode) {
		t.Fatalf("missing tools classification: %v", err)
	}
}

func TestVideoRealFFmpeg(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg unavailable")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	file := filepath.Join(t.TempDir(), "fixture.mp4")
	cmd := exec.CommandContext(ctx, ffmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=16x16:r=4", "-t", "2", "-threads", "1", "-c:v", "mpeg4", file)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create local video: %v %s", err, output)
	}
	frames, err := decodeVideoFrames(ctx, ffmpeg, ffprobe, file)
	if err != nil || frames != 4 {
		t.Fatalf("real frame count=%d err=%v", frames, err)
	}
	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	counter, cancelCounter := newVideoFrameCounter(ctx)
	defer cancelCounter()
	frames, err = counter("data:video/mp4;base64," + base64.StdEncoding.EncodeToString(content))
	if err != nil || frames != 4 {
		t.Fatalf("inline frame count=%d err=%v", frames, err)
	}
}
