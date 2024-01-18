# Project: Acquiring and Processing Information on World's Largest Banks

## Used Libraries and Framework

- **requests**: Used for accessing information from the URL.
- **bs4 (BeautifulSoup)**: Used for web scraping.
- **pandas**: Used for processing extracted data, storing it in required formats, and communicating with databases.
- **sqlite3**: Required to create a database server connection.
- **numpy**: Required for mathematical rounding operations.
- **datetime**: Contains the `datetime` function used for extracting timestamps for logging purposes.

## Overview

In this project, I put all the skills acquired and my knowledge of basic Python to the test. I used real-world data and performed the operations of Extraction, Transformation, and Loading (ETL) as required.

I created a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Furthermore, the data needs to be transformed and stored in GBP, EUR, and INR, in accordance with the exchange rate information available as a CSV file. The processed information table is to be saved locally in CSV format and as a database table.
