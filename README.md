This project contains the tools used in the research based on Reddit community discussions. The study illustrates, through examples, how a crisis situation affects user engagement and activity, demonstrates the distribution of comment topics, and conducts an analysis of shifts in public sentiment and attitudes toward the government.



Reddit was selected as a primary open source of community discussion in order to analyze the distribution of behavior patterns and disputes within community interactions.



## Data Source



The raw data (Pushshift) is stored on [Academic Torrents](https://academictorrents.com/details/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1). 

The analysis uses the following monthly files:

- `RS_2025-01.zst` (submissions January 2025)

- `RC_2025-01.zst` (comments January 2025)



Base extraction scripts are adapted from [PushshiftDumps](https://github.com/SanGreel/PushshiftDumps/tree/master).



**Final Dataset:** The processed dataset can be found [here](ВСТАВТЕ_ТУТ_ВАШЕ_ПОСИЛАННЯ).



## Project Structure & Scripts



The repository includes the following Python scripts for data extraction and cleaning:



* **`combine_folder_multiprocess.py`** Unpacking monthly files and downloading selected subreddits into a selected directory.

    

* **`filter_file_categorized.py`** Processing `.zst` files and downloading data into a CSV file with specific fields (`base_headers`). Enables (but does not require) the creation of additional categorized columns using a keyword dictionary.



* **`data_processing.ipynb`** Data cleaning and dataset downloading operations.



## How to Run



Before running the notebooks through **Google Colab**, make sure you have copied the dataset to your Drive. To run it using **Jupyter** or another local environment, the dataset should be loaded locally.



### Notebooks Description



1.  **`basic_metrics`** Dataset overview and general statistics.



2.  **`eda`** (Exploratory Data Analysis)  

    Visualize, describe, and interpret patterns using data. Includes making assumptions, asking questions, and using visualizations to provide answers or draw conclusions.



3.  **`R-language`** Part of EDA using R-language as an effective tool for language processing.

    > **Note:** This notebook runs on an R kernel. It cannot be run in the same notebook environment as Python on a local setup without configuration. To run it in Google Colab, use `%%R` at the start of the cell. We strongly recommend running it separately on a dedicated R kernel.# reddit_analysis_project
This project explores the dynamics of public opinion on the Reddit platform during major winter fires in the Los Angeles area in January 2025. The goal of the study will be to find out how the focus of discussions changes in response to extreme situations.

The research analyzes 3.59M comments over 4 months to identify how attention shifted across topics — from fire → humanitarian aid → government →  politics → conspiracy narratives.

The project includes data preprocessing, categorization, topic modeling (TF-IDF), sentiment trends,  
and visual interpretation of behavioral crisis dynamics.
