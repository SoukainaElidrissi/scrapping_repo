import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

# Function to scrape journal data from SCImago
def scrape_journal_data(journal_name, driver):
    try:
        # Step 1: Access SCImago website
        driver.get("https://www.scimagojr.com/")
        wait = WebDriverWait(driver, 10)

        # Step 2: Search for the journal by name
        search_box = wait.until(EC.visibility_of_element_located((By.ID, "searchinput")))
        search_box.clear()
        search_box.send_keys(journal_name)
        search_box.send_keys(Keys.RETURN)

        # Step 3: Select the journal link
        journal_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'journalsearch.php') and contains(@href, 'sid')]")))
        journal_link.click()

        # Step 4: Close any pop-up ad if present
        try:
            close_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "ns-jhssl-e-5.close-button")))
            close_button.click()
        except Exception:
            pass  # No ad to close, continue

        # Step 5: Click the table button to view quartile data
        table_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".combo_buttons .combo_button.table_button")))
        table_button.click()
        time.sleep(2)  # Allow table to load

        # Step 6: Scrape table data containing Quartiles
        table = driver.find_element(By.XPATH, "//div[@class='cellslide']/table")
        rows = table.find_elements(By.XPATH, ".//tbody/tr")
        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) == 3:
                category, year, quartile = [col.text.strip() for col in cols]
                year = int(year)  # Convert the year to integer
                data.append({"Year": year, "Quartile": quartile})

        return data

    except Exception as e:
        print(f"Error occurred while scraping data for {journal_name}: {e}")
        return []

# Function to find and append Quartile data to article's publisher
def append_quartile_to_article(original_data, driver):
    # Iterate over each article in the original data
    for article in original_data:
        journal_name = article.get('publisher', {}).get('name', '').strip()
        ISSN = article.get('publisher', {}).get('ISSN', '').strip()
        year = article.get('Year', 0)

        # Skip if Quartile is already present
        if article.get('publisher', {}).get('Quartile'):
            continue

        # Get the quartile data for the journal from SCImago
        quartile_data = scrape_journal_data(journal_name, driver)

        # Initialize Quartile as an empty string
        article['publisher']['Quartile'] = ''

        # Try to find the quartile for the article's year, and if not found, fallback to previous years
        if quartile_data:
            for i in range(0, 10):  # Check the current year and up to 9 previous years
                target_year = year - i
                for entry in quartile_data:
                    if entry['Year'] == target_year:
                        article['publisher']['Quartile'] = entry['Quartile']
                        break
                if article['publisher']['Quartile']:  # Exit once quartile is found
                    break

    return original_data

# Main execution block
def main():
    # Initialize the Selenium WebDriver (Make sure to add path to ChromeDriver)
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode if UI is not required
    driver = webdriver.Chrome(options=options)

    # Load original data from IoTi.json file
    try:
        with open('DevOpsi.json', 'r', encoding='utf-8') as f:
            original_data = json.load(f)
    except UnicodeDecodeError as e:
        print(f"Error decoding the JSON file: {e}")
        return

    # Append Quartile to publisher for each article
    updated_data = append_quartile_to_article(original_data, driver)

    # Check if IoT.json exists and append data
    if os.path.exists('DevOps.json'):
        with open('DevOps.json', 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
        existing_data.extend(updated_data)
        with open('DevOps.json', 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=4)
    else:
        with open('DevOps.json', 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, indent=4)

    # Close the driver after scraping
    driver.quit()

# Run the script
if __name__ == "__main__":
    main()
