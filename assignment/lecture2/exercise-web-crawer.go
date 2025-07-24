package main

import (
	"fmt"
	"sync"
	"time"
)

type SafeCounter struct {
	mu sync.Mutex
	v map[string]int
}

func (c *SafeCounter) Inc(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v[s]++
}

func (c *SafeCounter) Get(s string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v[s]
}

type Fetcher interface {
	// Fetch 返回 URL 所指向页面的 body 内容，
	// 并将该页面上找到的所有 URL 放到一个切片中。
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl 用 fetcher 从某个 URL 开始递归的爬取页面，直到达到最大深度。
func Crawl(url string, depth int, fetcher Fetcher, cnt *SafeCounter) {
	if cnt.Get(url) > 0 {
		return
	}
	cnt.Inc(url)
	
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		go Crawl(u, depth-1, fetcher, cnt)
	}
	return
}

func main() {
	cnt := SafeCounter{v: make(map[string]int)}
	go Crawl("https://golang.org/", 4, fetcher, &cnt)
	time.Sleep(time.Second)
}

// fakeFetcher 是待填充结果的 Fetcher。
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher 是填充后的 fakeFetcher。
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
