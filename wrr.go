package wrr

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/pkg/errors"
)

const (
	// DefalutBtreeBorder is choose value config
	DefalutBtreeBorder = 10
)

// Client is XXX
type Client struct {
	DataList    []Data
	Weights     uint
	BtreeBorder uint
	ListNum     uint
}

// Data is Round Robin data
type Data struct {
	Key    string
	Value  string
	Weight uint
	Range  uint
}

// Config is XXX
type Config struct {
	BtreeBorder uint
	List        []Data
}

// NewClient is XXX
func NewClient(c Config) (*Client, error) {
	btreeBorder := c.BtreeBorder
	if btreeBorder == 0 {
		btreeBorder = DefalutBtreeBorder
	}

	client := &Client{
		BtreeBorder: btreeBorder,
	}

	err := client.Set(c.List)
	if err != nil {
		return nil, errors.Wrap(err, "initialize data list set error:")
	}

	return client, nil

}

func checkValue(data Data) (Data, error) {
	if data.Value == "" {
		return Data{}, fmt.Errorf("[error] set data value is empty")
	}

	if data.Key == "" {
		data.Key = data.Value
	}

	return data, nil
}

// Set is set round robin data
func (cli *Client) Set(list []Data) error {
	if len(list) == 0 {
		return nil
	}

	checked := make(map[string]Data, len(list))
	keys := make([]string, 0, len(list))

	var listErr error
	for _, data := range list {
		data, err := checkValue(data)
		if err != nil {
			listErr = errors.Wrap(err, "Set error:")
			break
		}

		if _, ok := checked[data.Key]; ok {
			listErr = fmt.Errorf("Set error. Can not set same Key name:")
			break
		}

		checked[data.Key] = data
		keys = append(keys, data.Key)
	}

	if listErr != nil {
		return listErr
	}

	weights := uint(0)
	dataList := make([]Data, 0, len(list))

	sort.Strings(keys)
	for i, key := range keys {
		data := checked[key]
		data.Range = weights

		if i == 0 {
			dataList = append(dataList, data)
		} else {
			// unshift
			dataList, dataList[0] = append(dataList[:1], dataList[0:]...), data
		}

		weights += data.Weight
	}

	cli.DataList = dataList
	cli.Weights = weights
	cli.ListNum = uint(len(dataList))

	return nil
}

// Add is add data
func (cli *Client) Add(d Data) error {
	d, err := checkValue(d)
	if err != nil {
		return errors.Wrap(err, "Add error:")
	}

	dataList := make([]Data, 0)
	dataList = append(dataList, cli.DataList...)

	isAdded := true
	for _, data := range dataList {
		if data.Key == d.Key {
			err = fmt.Errorf("[error] Already exists %s value", d.Key)
			isAdded = false
			break
		}
	}

	if err != nil {
		return err
	}

	if isAdded {
		dataList = append(dataList, d)
		cli.Set(dataList)
	}

	return nil
}

// Replace is replace exist data
func (cli *Client) Replace(d Data) error {
	d, err := checkValue(d)
	if err != nil {
		return errors.Wrap(err, "Replace error:")
	}

	dataList := cli.DataList
	replaced := false

	for i, data := range dataList {
		if data.Key == d.Key {
			dataList[i] = d
			replaced = true
			break
		}
	}

	if replaced {
		cli.Set(dataList)
	}

	return nil
}

// Remove is remove exist data
func (cli *Client) Remove(key string) error {
	list := cli.DataList
	newList := make([]Data, 0)
	removed := false

	for _, data := range list {
		if key != data.Key {
			newList = append(newList, data)
		} else {
			removed = true
		}
	}

	if removed {
		cli.Set(newList)
	}

	return nil
}

// Next is return data by B-Tree
func (cli *Client) Next() (Data, error) {
	if cli.ListNum == 0 {
		return Data{}, fmt.Errorf("[error] Not set List")
	}

	start, end := uint(0), cli.ListNum-1

	myRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	if cli.Weights == 0 {
		return cli.DataList[myRand.Intn((len(cli.DataList)))], nil
	}

	weight := uint(myRand.Intn(int(cli.Weights)))
	if cli.ListNum < cli.BtreeBorder {
		for _, data := range cli.DataList {
			if weight >= data.Range {
				return data, nil
			}
		}
	} else {
		for start < end {
			mid := uint((start + end) / 2)
			if cli.DataList[mid].Range <= weight {
				end = mid
			} else {
				start = mid + uint(1)
			}
		}
		return cli.DataList[start], nil
	}

	return Data{}, fmt.Errorf("[error] Not matching vbalue")
}
