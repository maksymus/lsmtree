package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	lmstree "github.com/maksymus/lmstree"
	"github.com/peterh/liner"
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

	line := liner.NewLiner()
	defer line.Close()
	line.SetCtrlCAborts(true)

	fmt.Println("lsmtree — type 'help' for commands")

	for {
		input, err := line.Prompt("> ")
		if err != nil { // EOF or Ctrl-C
			break
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		line.AppendHistory(input)

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
