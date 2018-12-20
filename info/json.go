// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Jul-02 11:10 (EST)
// Function:

package info

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

func SaveJson(file string, dat interface{}) error {

	temp := file + ".tmp"

	js, _ := json.Marshal(dat)

	fd, err := os.Create(temp)
	if err != nil {
		return err
	}

	fd.Write(js)
	fd.Close()
	os.Rename(temp, file)

	return nil
}

func LoadJson(file string, dat interface{}) error {

	js, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	err = json.Unmarshal(js, dat)
	if err != nil {
		return err
	}

	return nil
}
