package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strings"
	"sync"
)

const maxSegmentLength = 100
const maxBuckets = 10_000

var runners = runtime.NumCPU() - 1
var bufferSize = os.Getpagesize() * 1024 * 8 // read 8K pages at a time
var readerWg sync.WaitGroup
var resultWg sync.WaitGroup

type Measurement struct {
	Max   int
	Min   int
	Count int
	Total int64
}

func (m *Measurement) AddValue(v int) {
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
	replyChan := make(chan map[string]*Measurement, 5000)
	resultWg.Add(1)
	// start the result handler
	go func() {
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
					store[k] = *v
				}
			}
		}
		handleResults(store)
		resultWg.Done()
	}()

	ncfr := NewNoCopyFileReader(input, '\n')
	for i := 0; i < runners; i++ {
		readerWg.Add(1)
		go func() {
			for {
				d := make([]byte, bufferSize+maxSegmentLength)
				n, err := ncfr.Read(d)
				if n == 0 {
					break
				}
				if err != nil {
					panic(err)
				}
				handleChunk(d, replyChan)
			}
			readerWg.Done()
		}()
	}

	readerWg.Wait()
	close(replyChan)
	resultWg.Wait()

}

func handleChunk(d []byte, result chan map[string]*Measurement) {
	s := make(map[string]*Measurement, 1000)
	pos := 0
	lineStartPos := pos
	delPos := -1

	var name []byte
	for pos < len(d) {
		switch d[pos] {
		case ';':
			delPos = pos
			name = d[lineStartPos:pos]
		case '\n':
			lineStartPos = pos + 1 // start of next line
			// do end of line processing
			value := parseInt(d[delPos+1 : pos])
			if m, ok := s[string(name)]; ok {
				m.AddValue(value)
			} else {
				s[string(name)] = &Measurement{Max: value, Min: value, Count: 1, Total: int64(value)}
			}
		}
		pos++
	}
	result <- s
}

func handleResults(m map[string]Measurement) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var sb strings.Builder
	for _, k := range keys {
		v := m[k]
		sb.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", k, tenthFloat(v.Min), tenthFloat(v.Max), tenthFloat(int(v.Total/int64(v.Count)))))
	}
	fmt.Println("{" + sb.String()[:sb.Len()-2] + "}")
}

func parseInt(in []byte) int {
	var v = 0
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

func tenthFloat(in int) float64 {
	return math.Round(float64(in)) / 10
}

// NoCopyFileReader is a file reader that does not copy the buffer
type NoCopyFileReader struct {
	sync.Mutex
	r         io.Reader
	leftover  []byte
	delimiter byte
}

func NewNoCopyFileReader(r io.Reader, delimiter byte) *NoCopyFileReader {
	return &NoCopyFileReader{r: r, delimiter: delimiter}
}

func (ncfr *NoCopyFileReader) Read(d []byte) (int, error) {
	ncfr.Lock()
	defer ncfr.Unlock()

	offset := 0
	if len(ncfr.leftover) > 0 {
		copy(d, ncfr.leftover)
		offset = len(ncfr.leftover)
		ncfr.leftover = nil
	}

	n, err := ncfr.r.Read(d[offset : len(d)-maxSegmentLength])
	if err != nil {
		return n, err
	}
	n += offset

	if d[n-1] != ncfr.delimiter { // not complete line
		// find the last delimiter
		lastDelimiter := bytes.LastIndexByte(d[:n], ncfr.delimiter)
		if lastDelimiter == -1 {
			return n, fmt.Errorf("'%s' delimiter not found in the buffer", string(ncfr.delimiter))
		}
		ncfr.leftover = make([]byte, n-lastDelimiter-1)
		copy(ncfr.leftover, d[lastDelimiter+1:n]) // copy the leftover
		n = lastDelimiter + 1
		d = d[0:n]
	}
	return n, nil
}
