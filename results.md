# Results

1) the description of the scheduling,
2) the frequency of the deployment, meaning the number of times where the deployment occurs in the histrogram;
3) the deployment runtime
4) the user cost of the deployment
5) the battery lifetime of the deployment
6) the execution time of the algorithm

## Testing

### HEFT

```bash
-iter=5
-algoName=heft
```

1 4863.15067158967 921.9425960060483 26615.927067842167 6.24307562E7
1 4076.1571702466263 848.9814109287128 26618.428117013103 3.22590916E7

### Random offloading (from tutorial)

1 704.2662610170722 288.8271579683193 26513.772331011172 0.0

### HLFET

```bash
-iter=5
-algoName=hlfet
```

1 4403.061509523557 887.0131914926426 26618.126797755005 4.8299151E7 local
1 1004.1184058828775 209.36134264828414 26594.335255706457 1.7258552E7 cluster

### MCP

```bash
-iter=5
-nApps=5
-algoName=mcp
```

1 65.00000000000001 0.0 26639.95935875 6.79451502E8

### ETF

```bash
-iter=5
-algoName=etf
```

1 4478.196535889447 869.4221134183002 26445.34097288892 1.24508021E8
