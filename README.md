# bitcoin-block-scraper (Python 3.6+)
Generate Bitcoin datasets (blocks and their transactions) in JSON format. 

#### Parameters: 
You can enter the index or height of the first block as a parameter. (Default value: 190031)

### Download N blocks starting at selected height
```bash
python3 blockchain_scraper_height.py 56
```

#### NOTE:
Edit the file "blockchain_scraper_height.py" or "blockchain_scraper_index.py" to change the number of blocks to download (Default value is "BLOCKS_NO = 4").
