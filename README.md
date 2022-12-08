
## How to build

```shell
go build -v .
```


## How to run

```shell
./stability --config ./sample.yaml test --threads 2
```

## Expected output

```
2022/12/08 16:14:14 Using config file: ./sample.yaml 2
2022/12/08 16:14:14 Running stability test with [2] threads
2022/12/08 16:15:14 Still running...
2022/12/08 16:16:14 Still running...
2022/12/08 16:17:14 Still running...
...
```