package main

import (
	"os"
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

func main() {
	SaveData1("test.txt", []byte("hello"))
}
