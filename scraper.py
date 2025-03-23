import os
import time
import argparse
import pandas as pd
from typing import List, Dict, Optional, Any
import undetected_chromedriver as uc
from selenium.webdriver import ChromeOptions
from bs4 import BeautifulSoup
from tqdm import tqdm
from fastparquet import write
import logging
import sys
from datetime import datetime


def setup_logger(log_dir: str, log_file: str = None, log_level: int = logging.INFO) -> logging.Logger:
    """Configure and return a logger instance.

    Args:
        log_dir: Directory to store log files
        log_file: Optional log filename
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Generate default log filename if not provided
    if not log_file:
        log_file = f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Full path to log file
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger("bounty_scraper")
    logger.setLevel(log_level)
    logger.handlers = []  # Clear any existing handlers

    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Add file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logging to: {log_path}")

    return logger


class BountyTimelineScraper:
    """
    Scrapes timeline data for Stack Overflow questions with bounties.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the scraper with configuration parameters.

        Args:
            config: Dictionary containing configuration parameters
        """
        # Set configuration values from input dict with defaults
        self.input_dir = config.get('input_dir', './input_data')
        self.output_dir = config.get('output_dir', './output_data')
        self.ids_file = config.get('ids_file', 'bounty_question_ids.parquet')
        self.results_file = config.get('results_file', 'bounty_timeline_results.parquet')
        self.batch_size = config.get('batch_size', 1000)
        self.timeout = config.get('timeout', 15)
        self.delay = config.get('delay', 0.1)
        self.headless = config.get('headless', True)

        # Construct full paths
        self.ids_path = os.path.join(self.input_dir, self.ids_file)
        self.results_path = os.path.join(self.output_dir, self.results_file)

        # Create directory structure
        self._create_directories()

        # Initialize logger
        log_file = config.get('log_file')
        log_level = config.get('log_level', logging.INFO)
        self.logger = setup_logger(self.output_dir, log_file, log_level)

        # Initialize driver to None (will be set up when needed)
        self.driver = None

        self.logger.info(f"Initialized scraper with: input_dir={self.input_dir}, "
                         f"output_dir={self.output_dir}, batch_size={self.batch_size}")

    def _create_directories(self) -> None:
        """Ensure necessary data directories exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _get_unprocessed_question_ids(self) -> List[int]:
        """
        Get question IDs that have bounties but haven't been processed yet.

        Returns:
            List[int]: List of unprocessed question IDs
        """
        # Load all bounty question IDs from input directory
        if not os.path.exists(self.ids_path):
            self.logger.error(f"Bounty question IDs file not found: {self.ids_path}")
            return []

        all_bounty_ids_df = pd.read_parquet(self.ids_path)
        all_bounty_ids = set(all_bounty_ids_df['question_id'].tolist())
        self.logger.info(f"Loaded {len(all_bounty_ids)} total bounty question IDs from input directory")

        # Load already processed IDs from output directory
        processed_ids = set()
        if os.path.exists(self.results_path):
            try:
                processed_df = pd.read_parquet(self.results_path)
                processed_ids = set(processed_df['question_id'].tolist())
                self.logger.info(f"Found {len(processed_ids)} already processed question IDs in output directory")
            except Exception as e:
                self.logger.error(f"Error reading processed results file: {e}")

        # Get unprocessed IDs
        unprocessed_ids = list(all_bounty_ids - processed_ids)
        self.logger.info(f"Found {len(unprocessed_ids)} unprocessed question IDs")
        return unprocessed_ids

    def _configure_driver(self) -> None:
        """Configure and initialize the Selenium WebDriver."""
        chrome_options = ChromeOptions()

        if self.headless:
            chrome_options.add_argument("--headless")

        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")

        self.driver = uc.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(self.timeout)
        self.driver.set_script_timeout(self.timeout)
        self.logger.info(f"WebDriver initialized (headless={self.headless})")

    def _scrape_timeline_events(self, question_id: int) -> Optional[Dict[str, Any]]:
        """
        Scrape bounty start/end events for a given question ID.

        Args:
            question_id: The Stack Overflow question ID to scrape

        Returns:
            Dictionary containing question ID and bounty start/end dates or None if error
        """
        time.sleep(self.delay)
        url = f'https://stackoverflow.com/posts/{question_id}/timeline'

        try:
            self.driver.get(url)
            html_content = self.driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')

            bounty_start, bounty_end = [], []

            bounty_rows = soup.find_all(
                'tr',
                attrs={
                    'data-eventtype': 'history',
                    'class': lambda x: x and 'datehash' in x
                }
            )

            for row in bounty_rows:
                event_cell = row.find('td', class_='wmn1')
                date_cell = row.find('span', class_='relativetime')

                if event_cell and date_cell:
                    event_text = event_cell.get_text(strip=True)
                    date = date_cell.get('title')

                    # Clean the timestamp format - remove any surrounding characters
                    # The format from Stack Overflow is typically "2023-12-05 18:56:51Z"
                    if date:
                        # Remove any quotes, brackets, or other non-timestamp characters
                        date = date.strip('"[]\'')

                    print(date)

                    if 'bounty started' in event_text:
                        bounty_start.append(date)
                    if 'bounty ended' in event_text:
                        bounty_end.append(date)

            return {
                'question_id': question_id,
                'bounty_start': bounty_start,
                'bounty_end': bounty_end
            }

        except Exception as e:
            self.logger.error(f"Error scraping timeline for question_id={question_id}: {e}")
            return None

    def _save_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Save scraped results to parquet file in output directory.

        Args:
            results: List of dictionaries containing scraping results
        """
        if not results:
            return

        # Count statistics for batch summary
        questions_with_events = 0
        total_start_events = 0
        total_end_events = 0

        for result in results:
            start_count = len(result['bounty_start'])
            end_count = len(result['bounty_end'])
            total_start_events += start_count
            total_end_events += end_count

            if start_count > 0 or end_count > 0:
                questions_with_events += 1

        # Create and save the DataFrame
        df = pd.DataFrame(results)

        if os.path.exists(self.results_path):
            write(self.results_path, df, append=True)
        else:
            write(self.results_path, df)

        # Log batch statistics after saving
        self.logger.info(f"Batch statistics: {len(results)} questions processed, "
                        f"{questions_with_events} had bounty events "
                        f"({total_start_events} start events, {total_end_events} end events)")
        self.logger.info(f"Saved batch of {len(results)} results to {self.results_path}")

    def run(self) -> None:
        """Run the complete scraping pipeline."""
        start_time = datetime.now()
        self.logger.info(f"Starting Stack Overflow bounty timeline scraping at {start_time}")

        # Get question IDs to process
        question_ids = self._get_unprocessed_question_ids()
        if not question_ids:
            self.logger.info("No unprocessed question IDs found. Exiting.")
            return

        # Initialize WebDriver
        self._configure_driver()

        try:
            # Process question IDs in batches
            results = []
            with tqdm(total=len(question_ids), desc="Scraping Bounty Timelines") as pbar:
                for question_id in question_ids:
                    result = self._scrape_timeline_events(question_id)
                    if result:
                        results.append(result)

                    # Save when batch size is reached
                    if len(results) >= self.batch_size:
                        self._save_results(results)
                        results = []

                    pbar.update(1)

            # Save any remaining results
            if results:
                self._save_results(results)

            end_time = datetime.now()
            duration = end_time - start_time
            self.logger.info(f"Completed scraping all unprocessed question IDs")
            self.logger.info(f"Total execution time: {duration}")

        except Exception as e:
            self.logger.error(f"Unexpected error during scraping: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("WebDriver closed")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Stack Overflow Bounty Timeline Scraper',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--input-dir', type=str, default='./input_data',
                        help='Directory containing input data files')
    parser.add_argument('--output-dir', type=str, default='./output_data',
                        help='Directory to store output data files')
    parser.add_argument('--ids-file', type=str, default='bounty_question_ids.parquet',
                        help='Filename of bounty question IDs parquet file in input directory')
    parser.add_argument('--results-file', type=str, default='bounty_timeline_results.parquet',
                        help='Filename to store scraping results in output directory')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of results to collect before saving to disk')
    parser.add_argument('--timeout', type=int, default=15,
                        help='Timeout in seconds for web requests')
    parser.add_argument('--delay', type=float, default=0.1,
                        help='Delay in seconds between requests')
    parser.add_argument('--no-headless', action='store_true',
                        help='Disable headless mode for browser (shows UI)')
    parser.add_argument('--log-file', type=str, default=None,
                        help='Log filename (saved in output directory)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level')

    return parser.parse_args()


def main():
    """Main entry point for the scraper."""
    # Parse command-line arguments
    args = parse_arguments()

    # Convert log level string to logging constant
    log_level = getattr(logging, args.log_level)

    # Create configuration dictionary from arguments
    config = {
        'input_dir': args.input_dir,
        'output_dir': args.output_dir,
        'ids_file': args.ids_file,
        'results_file': args.results_file,
        'batch_size': args.batch_size,
        'timeout': args.timeout,
        'delay': args.delay,
        'headless': not args.no_headless,
        'log_file': args.log_file,
        'log_level': log_level
    }

    # Initialize and run the scraper
    scraper = BountyTimelineScraper(config)
    scraper.run()


if __name__ == "__main__":
    main()