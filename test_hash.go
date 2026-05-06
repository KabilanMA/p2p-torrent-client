package main

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"

	bencode "github.com/jackpal/bencode-go"
	"github.com/KabilanMA/p2p-torrent-client/internal/torrent"
)

func main() {
	f, _ := os.Open("input/ubuntu.torrent")
	defer f.Close()
	m, _ := bencode.Decode(f)
	
	dict := m.(map[string]interface{})
	info := dict["info"]
	
	var buf bytes.Buffer
	bencode.Marshal(&buf, info)
	hash := sha1.Sum(buf.Bytes())
	fmt.Printf("Map Hash: %x\n", hash)

	ti, _ := torrent.OpenFile("input/ubuntu.torrent")
	fmt.Printf("Struct Hash: %x\n", ti.InfoHash)
}