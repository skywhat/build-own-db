package main

import (
	"fmt"
	"os"

	"math/rand"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, rand.Intn(100000))
	fp, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data) // write data to tmp file
	if err != nil {
		return err
	}
	if err := fp.Sync(); err != nil { // sync data to disk
		return err
	}
	return os.Rename(tmp, path) // replace the original file
}

func main() {
	SaveData2("test.txt", []byte("hello"))
}
