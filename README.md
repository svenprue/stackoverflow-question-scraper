# Stack Overflow Bounty Timeline Scraper
This tool extracts bounty start/end timestamps from Stack Overflow questions by scraping their timeline pages. Stack Exchange data dumps don't include exact bounty timestamps, so this scraper accesses the timeline view to collect this data for research purposes.

## Setup
### Clone the repository
```bash
git clone https://github.com/yourusername/stackoverflow-bounty-scraper.git
cd stackoverflow-bounty-scraper
```

### Install dependencies
```bash
pip install pandas undetected-chromedriver selenium beautifulsoup4 tqdm fastparquet
```

### Input Data
The scraper requires a parquet file containing question IDs to be scraped. This file should be placed in the input_data directory.
```
input_data/
└── bounty_question_ids.parquet
    └── Schema: ['question_id': int]
```
The provided input dataset contains all bountied question IDs from Stack Overflow up to January 5, 2025.

# Usage

## Basic usage with default settings
```python
python scraper.py
```

## Custom configuration
```python
python scraper.py --input-dir ./custom_input --output-dir ./custom_output --batch-size 500 --delay 0.5
```

### Command-line options
```
--input-dir      Directory containing input data files
--output-dir     Directory to store output data files
--ids-file       Filename of bounty question IDs parquet file
--results-file   Filename to store scraping results
--batch-size     Number of results to collect before saving
--timeout        Timeout in seconds for web requests
--delay          Delay in seconds between requests
--no-headless    Disable headless mode for browser
--log-file       Log filename (saved in output directory)
--log-level      Logging level
```

# Output
Results are saved as a parquet file in the output directory:
```
Copyoutput_data/
├── bounty_timeline_results.parquet
│   └── Schema: ['question_id': int, 'bounty_start': list, 'bounty_end': list]
└── scraper_YYYYMMDD_HHMMSS.log
```
Note that bounty_start and bounty_end are stored as lists because Stack Overflow questions can be bountied multiple times throughout their lifecycle. Each element in these lists represents a timestamp for a separate bounty event on the same question.

# Notes
The scraper can be easily modified to extract other question-level timestamps available in the timeline view, such as:
- When a question was marked as accepted
- When a question was closed or reopened
To this end, simply update the event filtering logic in the _scrape_timeline_events method.

# Disclaimer
This scraper is intended for research purposes only. Please respect Stack Overflow's terms of service and rate limits when using this scraper.