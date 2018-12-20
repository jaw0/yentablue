// Copyright (c) 2018
// Author: Jeff Weisberg <jaw @ tcp4me.com>
// Created: 2018-Nov-16 14:38 (EST)
// Function: at-rest encryption

package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"

	"golang.org/x/crypto/pbkdf2"
)

const (
	SALT = "drink ovaltine"
	ITER = 4099
)

type Box struct {
	key []byte
}

func New(pass string) *Box {
	// convert user supplied password -> 256 bit key
	dk := pbkdf2.Key([]byte(pass), []byte(SALT), ITER, 32, sha256.New)
	return &Box{dk}
}

func (b *Box) mkCipher(tweak []byte) (cipher.AEAD, error) {
	// AES-256/GCM

	k := b.key
	if len(tweak) > 0 {
		k = make([]byte, len(b.key))
		for i, c := range b.key {
			if i < len(tweak) {
				k[i] = c ^ tweak[i]
			} else {
				k[i] = c
			}
		}
	}

	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return aesgcm, nil
}

func (b *Box) Encrypt(ptext []byte, tweak []byte, additional []byte) ([]byte, error) {

	c, err := b.mkCipher(tweak)
	if err != nil {
		return nil, err
	}

	// allocate space for nonce+ciphertext+mac
	buf := make([]byte, c.NonceSize()+c.Overhead()+len(ptext))
	nonce := buf[:c.NonceSize()]
	rand.Read(nonce)

	// encrypt
	// NB - mac (aka tag) is attached to ctext
	ciphertext := c.Seal(nil, nonce, ptext, additional)
	copy(buf[c.NonceSize():], ciphertext)

	return buf, nil
}

func (b *Box) Decrypt(ctext []byte, tweak []byte, additional []byte) ([]byte, error) {

	c, err := b.mkCipher(tweak)
	if err != nil {
		return nil, err
	}

	nonce := ctext[:c.NonceSize()]
	ctext = ctext[c.NonceSize():]

	plaintext, err := c.Open(nil, nonce, ctext, additional)
	return plaintext, err
}
