package wrr

import (
	"reflect"
	"testing"
)

func TestNewClient(t *testing.T) {
	cli, err := NewClient(Config{})
	if err != nil {
		t.Fatal("[error] initialize:", err.Error())
	}

	if cli.ListNum != 0 || len(cli.DataList) != 0 {
		t.Fatalf("[error] initializee list num %d, data list count %d", cli.ListNum, len(cli.DataList))
	}

	if cli.BtreeBorder != DefalutBtreeBorder {
		t.Fatalf("[error] initialize btree config %d", cli.BtreeBorder)
	}

	dataList := []Data{
		Data{Value: "dummy01", Weight: 1},
		Data{Value: "dummy02", Weight: 2},
	}

	cli, err = NewClient(Config{
		List:        dataList,
		BtreeBorder: 20,
	})
	if err != nil {
		t.Fatal("[error] initialize:", err.Error())
	}

	if cli.ListNum != uint(len(cli.DataList)) || len(cli.DataList) != len(dataList) {
		t.Fatalf("[error] initialize list num %d, data list count %d", cli.ListNum, len(cli.DataList))
	}

	if cli.Weights != 3 {
		t.Fatal("[error] client weights")
	}

	if cli.BtreeBorder != 20 {
		t.Fatalf("[error] initialize btree config %d", cli.BtreeBorder)
	}
}

func TestCheckValue(t *testing.T) {
	data := Data{
		Weight: 10,
		Value:  "fuga",
		Key:    "fugahoge",
	}

	d, err := checkValue(data)
	if err != nil {
		t.Fatal("[error] checkValue:", err.Error())
	}

	if !(reflect.DeepEqual(data, d)) {
		t.Fatal("[error] checkValue")
	}

	data2 := Data{
		Weight: 10,
		Value:  "fuga",
	}
	d, err = checkValue(data2)
	if err != nil {
		t.Fatal("[error] checkValue:", err.Error())
	}
	if d.Key != data2.Value {
		t.Fatalf("[error] Key will automatically become Value data: %s", d.Key)
	}

	data3 := Data{
		Weight: 10,
		Key:    "fuga",
	}
	d, err = checkValue(data3)
	if err == nil {
		t.Fatal("[error] Can not set Value's empty data")
	}
}

func TestSet(t *testing.T) {
	t.Run("Set list", func(t *testing.T) {
		cli, _ := NewClient(Config{})
		dataList := []Data{
			Data{Weight: 10, Value: "dummy01"},
		}

		err := cli.Set(dataList)
		if err != nil {
			t.Fatalf("[error] Set data list: %s", err.Error())
		}

		if len(cli.DataList) != 1 {
			t.Fatalf("[error] Set data list")
		}
	})

	t.Run("Set some data list", func(t *testing.T) {
		cli, _ := NewClient(Config{})
		dataList := []Data{
			Data{Weight: 10, Value: "dummy01"},
			Data{Weight: 20, Value: "dummy02"},
			Data{Weight: 30, Value: "dummy03"},
			Data{Weight: 40, Value: "dummy04"},
		}

		err := cli.Set(dataList)
		if err != nil {
			t.Fatalf("[error] Set data list: %s", err.Error())
		}

		if len(cli.DataList) != len(dataList) && cli.ListNum != uint(len(dataList)) {
			t.Fatalf("[error] Set data list")
		}
	})

	t.Run("Fail set same value data list", func(t *testing.T) {
		cli, _ := NewClient(Config{})
		dataList := []Data{
			Data{Weight: 10, Value: "dummy01"},
			Data{Weight: 20, Value: "dummy01"},
			Data{Weight: 30, Value: "dummy03"},
		}

		err := cli.Set(dataList)
		if err == nil {
			t.Fatalf("[error] Can not set same value data list")
		}
	})
}

func TestAdd(t *testing.T) {
	t.Run("Add data", func(t *testing.T) {
		cli, _ := NewClient(Config{})

		if len(cli.DataList) != 0 && cli.ListNum != 0 {
			t.Fatal("[error] Init DataList num")
		}

		data := Data{
			Weight: 100,
			Value:  "dummy01",
		}

		err := cli.Add(data)
		if err != nil {
			t.Fatal("[error] Add Data:", err.Error())
		}

		if len(cli.DataList) != 1 && cli.ListNum != 1 {
			t.Fatal("[error] Add Data Failed")
		}

		t.Run("Fail over ride data", func(t *testing.T) {
			err = cli.Add(data)
			if err == nil {
				t.Fatal("[error] Can not override same key name data.")
			}

		})
		data2 := Data{
			Weight: 20,
			Value:  "dummy02",
		}

		err = cli.Add(data2)
		if err != nil {
			t.Fatal("[error] Add Data:", err.Error())
		}

		if len(cli.DataList) != 2 && cli.ListNum != 2 {
			t.Fatal("[error] Add Data Failed")
		}
	})
}

func TestReplace(t *testing.T) {
	t.Run("Replace data", func(t *testing.T) {
		data := Data{
			Weight: 100,
			Value:  "dummy01",
		}
		data2 := Data{
			Weight: 200,
			Value:  "dummy02",
		}

		cli, err := NewClient(Config{
			List: []Data{data, data2},
		})

		if err != nil || len(cli.DataList) != 2 && cli.ListNum != 2 {
			t.Fatal("[error] Init DataList num")
		}

		data.Weight = 200
		err = cli.Replace(data)
		if err != nil {
			t.Fatal("[error] Replace data:", err.Error())
		}

		for _, d := range cli.DataList {
			if d.Weight != 200 {
				t.Fatal("[error] Replace data")
			}
		}
	})
}

func TestRemove(t *testing.T) {
	t.Run("Remove data", func(t *testing.T) {
		data := Data{
			Weight: 100,
			Value:  "dummy01",
		}
		data2 := Data{
			Weight: 200,
			Value:  "dummy02",
		}

		cli, err := NewClient(Config{
			List: []Data{data, data2},
		})

		if err != nil || len(cli.DataList) != 2 && cli.ListNum != 2 {
			t.Fatal("[error] Init DataList num")
		}

		err = cli.Remove("dummy01")
		if err != nil {
			t.Fatal("[error] Remove error:", err.Error())
		}

		if len(cli.DataList) != 1 && cli.ListNum != 1 {
			t.Fatal("[error] Remove error")
		}

		t.Run("not exits data", func(t *testing.T) {
			err := cli.Remove("hoge")
			if err != nil {
				t.Fatal("[error] Remove error:", err.Error())
			}

			if len(cli.DataList) != 1 && cli.ListNum != 1 {
				t.Fatal("[error] Remove error")
			}

		})

	})

}

func TestNext(t *testing.T) {
	cli, err := NewClient(Config{})
	if err != nil {
		t.Fatal("[error] NewClient:", err.Error())
	}
	_, err = cli.Next()
	if err == nil {
		t.Fatal("[error] empty case")
	}

	err = cli.Add(Data{Value: "dummy01", Weight: 10})
	if err != nil {
		t.Fatal(err.Error())
	}

	for i := 1; i < 10; i++ {
		if data, _ := cli.Next(); data.Value != "dummy01" {
			t.Fatal("[error] Next")
		}
	}
}
