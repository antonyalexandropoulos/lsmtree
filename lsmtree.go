package lsmtree

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

type keyType = int
type valueType = int

type Lsm struct {
	blockSize int
	k         int
	nodeSize  int
	nextEmtpy int
	block     []node
	disk1     string
	sorted    bool
}

type nodei struct {
	node  *node
	index int
}

type node struct {
	key keyType
	val valueType
}

//InitNewLsm : Returns a new lsm tree
func InitNewLsm(bufferSize int, sorted bool, fileName string) *Lsm {

	var tree Lsm
	tree.blockSize = bufferSize
	tree.k = 2
	tree.nextEmtpy = 0
	var temp node
	var size = int(unsafe.Sizeof(temp))
	tree.nodeSize = size
	tree.block = make([]node, tree.blockSize)
	tree.disk1 = fileName
	tree.sorted = sorted

	return &tree

}

func searchBuffer(k keyType, tree *Lsm) (*nodei, error) {

	for i := 0; i < tree.nextEmtpy; i++ {
		if tree.block[i].key == k {
			var node nodei
			node.index = i
			node.node.key = k
			node.node.val = tree.block[i].val

			return &node, nil
		}
	}
	return nil, errors.New("Key not found in the buffer")

}

func searchDisk(k keyType, tree *Lsm) (*nodei, error) {

	diskData := getDiskData(tree)

	for i := 0; i < len(diskData); i++ {
		if diskData[i].key == k {
			var node nodei
			node.index = i
			node.node.key = k
			node.node.val = tree.block[i].val

			return &node, nil
		}
	}
	return nil, errors.New("Key not found in the disk")

}

func getDiskData(tree *Lsm) []node {
	f, err := os.OpenFile(tree.disk1, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	var diskData []node
	for fileScanner.Scan() {
		splitString := strings.Split(fileScanner.Text(), " ")
		k, _ := strconv.Atoi(splitString[0])
		v, _ := strconv.Atoi(splitString[1])
		diskData = append(diskData, node{key: k, val: v})

	}
	return diskData
}

func writeDataToFile(tree *Lsm, allData []node) {
	f, err := os.OpenFile(tree.disk1, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	datawriter := bufio.NewWriter(f)

	for _, data := range allData {
		d := fmt.Sprintf("%d %d\n", data.key, data.val)
		_, _ = datawriter.WriteString(d)
	}

	datawriter.Flush()
}

func Put(key keyType, val valueType, tree *Lsm) {
	if tree.nextEmtpy == tree.blockSize {
		writeToDisk(tree)
	}
	var n node
	n.key = key
	n.val = val
	tree.block[tree.nextEmtpy] = n
	tree.nextEmtpy++

}

func Get(key keyType, tree *Lsm) (*node, error) {
	ni, _ := searchBuffer(key, tree)

	if ni != nil {
		return ni.node, nil
	}
	diskNode, _ := searchDisk(key, tree)

	if diskNode != nil {
		return diskNode.node, nil
	}

	return nil, errors.New("Key not found in the buffer or the disk")

}

func Delete(key keyType, tree *Lsm) {
	ni, _ := searchBuffer(key, tree)

	if ni != nil {
		tree.nextEmtpy--
		tree.block = append(tree.block[:ni.index], tree.block[ni.index+1:]...)
		return
	}

	dn, _ := searchDisk(key, tree)

	if dn != nil {
		diskData := getDiskData(tree)
		diskData = append(diskData[:ni.index], diskData[ni.index+1:]...)
		writeDataToFile(tree, diskData)
	}

}
func merge(whole *[]node, left *[]node, right *[]node) {
	n, m := len(*left), len(*right)
	i, j, k := 0, 0, 0

	for i < n && j < m {
		if (*left)[i].key < (*right)[i].key {
			(*whole)[k] = (*left)[i]
			i++
		} else {
			(*whole)[k] = (*right)[i]
			j++
		}
		k++
	}

	for i < n {
		(*whole)[k] = (*left)[i]
		k++
		i++
	}

	for j < m {
		(*whole)[k] = (*right)[i]
		k++
		j++
	}

}

//MergeSort : sorts the tree block
func MergeSort(block *[]node) {
	var n = len(*block)
	mid := n / 2

	if n < 2 {
		return
	}
	left, right := (*block)[:mid], (*block)[mid:]
	MergeSort(&left)
	MergeSort(&right)

	merge(block, &left, &right)

}

func writeToDisk(tree *Lsm) {

	if tree.sorted {
		MergeSort(&tree.block)
	}
	diskData := getDiskData(tree)

	var allData = make([]node, len(diskData)+tree.nextEmtpy)
	buffer := tree.block
	merge(&allData, &diskData, &buffer)

	go writeDataToFile(tree, allData)
	tree.nextEmtpy = 0

}

func PrintBufferData(tree *Lsm) {
	for i := 0; i < tree.nextEmtpy; i++ {
		node := tree.block[i]
		fmt.Printf("Key:%d Val:%d", node.key, node.val)
	}
}

func PrintDiskData(tree *Lsm) {
	diskData := getDiskData(tree)

	for i := 0; i < len(diskData); i++ {
		node := diskData[i]
		fmt.Printf("Key:%d Val:%d", node.key, node.val)
	}
}
