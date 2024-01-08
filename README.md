# Hugging Face Hub Scraper

Small program to scrape model repository data from the [Hugging Face Hub](https://huggingface.co/models), including the full history of `README.md` files.  
Kudos to [@Fresh-P](https://github.com/Fresh-P) who is responsible for a bulk of the inital implementation.

For an exploratory analysis of the data see my [website](https://simonschoelzel.rbind.io/project/ai-transparency/).

To run the program consider the following information:
- Download statistics refer to downloads over the past 30 days.
- The program leverages the `requests` package to obtain the HTML pages. Consider using cookies to ensure that the request considers your login information. That way, it will be possible to scrape repositories that require access permission which can be requested beforehand. The cookies ensure that you identify as a user with granted permission rights. The cookies file should be stored in the main folder and named `cookies`.
- If the field `commit_history` in the meta-file is empty, the repository likely requires permission rights
- If the field `commit_history` in the meta-file contains a `4xx` status code, it is likely the result of a `requests` error.
- The first time you run `main.py` it collects a list of all available model repositories. It will keep that exact same list unless you delete the `links.txt` file.
- Every time you run `main.py`, it checks which repositories from the `links.txt` have already been scraped (by cross-checking with the meta-file(s)). It only retains the repository links which have not yet been scraped. In addition, it retries scraping all links where the `commit_history` field in the meta-file contains an error code or is empty (that way, you may request permission to access certain repositories and retry scraping that repository).
