package helpers

import "bufio"

func ReadFull(b *bufio.Reader, p []byte) (n int, err error) {
	length := len(p)
	total := 0

	for total < length {
		n, err = b.Read(p[total:])
		if err != nil {
			return n, err
		}

		total += n
	}

	return total, nil
}
