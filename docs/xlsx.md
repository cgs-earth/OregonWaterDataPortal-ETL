# Xlsx

Xlsx data can be either crawled or manually added via the web interface.

## Manual

Run the xlsx cli command and follow the help output

```
python3 -m userCode.xlsx.main --help
```

### Loading

Supply the path to the xlsx file; it must conform to the template in order to be processed

```
python3 -m userCode.xlsx.main load userCode/xlsx/testdata/IoW_Reccomended_Obs_Data_Elements.xlsx
```
