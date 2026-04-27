package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	lmstree "github.com/maksymus/lmstree"
)

func main() {
	dir := flag.String("dir", "data", "data directory")
	flag.Parse()

	tree, err := lmstree.Open(lmstree.DefaultOptions(*dir))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer tree.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nbye")
		tree.Close()
		os.Exit(0)
	}()

	fmt.Println("lsmtree — type 'help' for commands")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 3)
		switch strings.ToLower(parts[0]) {
		case "put":
			if len(parts) < 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := tree.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			} else {
				fmt.Println("ok")
			}
		case "get":
			if len(parts) < 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			val, ok := tree.Get([]byte(parts[1]))
			if !ok {
				fmt.Println("(not found)")
			} else {
				fmt.Println(string(val))
			}
		case "delete", "del":
			if len(parts) < 2 {
				fmt.Println("usage: delete <key>")
				continue
			}
			if err := tree.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			} else {
				fmt.Println("ok")
			}
		case "help":
			fmt.Println("  put <key> <value>   store a key-value pair")
			fmt.Println("  get <key>           retrieve a value")
			fmt.Println("  delete <key>        delete a key")
			fmt.Println("  exit                quit")
		case "exit", "quit":
			return
		default:
			fmt.Fprintf(os.Stderr, "unknown command %q\n", parts[0])
		}
	}
}
