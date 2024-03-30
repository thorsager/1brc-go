BINARY := 1brc

.PHONY: build clean

build:
	go build -o $(BINARY) ./...

run: build
	time ./$(BINARY) -inputfile=measurements.txt

profile: build
	./$(BINARY) -inputfile=measurements.txt -cpuprofile=cpu.prof -memprofile=mem.prof -execprofile=exec.prof && go tool pprof -http=:8080 $(BINARY) cpu.prof


clean:
	rm -f $(BINARY)
	rm -f *.prof