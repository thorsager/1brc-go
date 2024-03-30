package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"sync"
)

const maxSegmentLength = 100
const maxBuckets = 10_000

var runners = runtime.NumCPU()
var bufferSize = os.Getpagesize() * 1024 * 8
var wg sync.WaitGroup

type Measurement struct {
	Max   int
	Min   int
	Count int
	Total int64
}

func (m Measurement) AddValue(v int) {
	m.Count++
	m.Total += int64(v)
	if v > m.Max {
		m.Max = v
	}
	if v < m.Min {
		m.Min = v
	}
}

var inputfile = flag.String("inputfile", "", "file from which to read measurements")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var executionprofile = flag.String("execprofile", "", "write tarce execution to `file`")

func main() {

	flag.Parse()

	if *executionprofile != "" {
		f, err := os.Create(*executionprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		trace.Start(f)
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	fh, err := os.Open(*inputfile)
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	doWork(fh)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal(err)
		}
	}

}

func doWork(input io.Reader) {
	fmt.Printf("runners=%d, buffer-size=%d\n", runners, bufferSize)

	replyChan := make(chan map[string]Measurement, runners)
	inputChunkChannel := make(chan []byte, runners)

	for i := 0; i < runners; i++ {
		wg.Add(1)
		go func() {
			fmt.Println("chunk worker started")
			for d := range inputChunkChannel {
				handleChunk2(d, replyChan)
			}
			fmt.Println("chunk worker done")
			wg.Done()
		}()
	}

	go func() {
		fmt.Println("reader started")
		ncfr := NewNoCopyFileReader(input, '\n')
		_ = ncfr.ChunkToChan(bufferSize+maxSegmentLength, inputChunkChannel)
		fmt.Println("reader done")
		close(inputChunkChannel)
		wg.Wait()
		close(replyChan)
	}()

	fmt.Println("waiting for results")
	store := make(map[string]Measurement, maxBuckets)
	for partialStore := range replyChan {
		for k, v := range partialStore {
			if m, ok := store[k]; ok {
				m.Count += v.Count
				m.Total += v.Total
				if v.Max > m.Max {
					m.Max = v.Max
				}
				if v.Min < m.Min {
					m.Min = v.Min
				}
			} else {
				store[k] = v
			}
		}
	}

	fmt.Println("Results:")
	keys := make([]string, 0, len(store))
	for k := range store {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		v := store[k]
		fmt.Printf("%s: count=%d, min=%d, max=%d, avg=%d\n", k, v.Count, v.Min, v.Max, v.Total/int64(v.Count))
	}
}

func handleChunk2(d []byte, result chan map[string]Measurement) {
	s := make(map[string]Measurement, 1000)
	pos := 0
	lineStartPos := pos
	delPos := -1

	name := ""
	for pos < len(d) {
		switch d[pos] {
		case ';':
			delPos = pos
			name = string(d[lineStartPos:pos])
		case '\n':
			lineStartPos = pos + 1 // start of next line
			// do end of line processing
			value := parseInt2(d[delPos+1 : pos])
			if m, ok := s[name]; ok {
				m.AddValue(value)
				s[name] = m
			} else {
				s[name] = Measurement{Max: value, Min: value, Count: 1, Total: int64(value)}
			}
		}
		pos++
	}
	result <- s
}

func parseInt2(in []byte) int {
	v := 0
	negative := false
	if in[0] == '-' {
		negative = true
		in = in[1:]
	}
	switch len(in) {
	case 3: // 1.2
		v = int(in[0]-'0')*10 + int(in[2]-'0')
	case 4: // 12.3
		v = int(in[0]-'0')*100 + int(in[1]-'0')*10 + int(in[3]-'0')
	}
	if negative {
		return -v
	}
	return v
}
