# Python API Data Transformation
## Project Description
This project involves retrieving JSON data via an API request and processing the response according to the specified requirements. The extracted data will be transformed into a structured tabular format to facilitate further analysis and integration into data pipelines.


### API Request Details<br>
🔹Endpoint: https://connection.keboola.com/v2/storage/components/keboola.python-transformation-v2/configs<br>
🔹Headers:<br>
	- X-StorageApi-Token: 9832-638325-OeNl9pQN0TKGlYaAetk7BWu4tNnZIORKHxNtCknG<br>
	- Content-Type: application/json

### Data Transformation Requirements<br>
🔹Extract the input tables from the following JSON path:<br>
configuration >> storage >> input >> tables<br>
🔹Represent each table within the nested array as an individual row in the output dataset.<br>
🔹Include the parent object identifier as a new column labeled "component_id".<br>
🔹Name the output file result_table.csv.<br>
🔹Create a new column called "id" and populate it with a unique identifier.

This transformation ensures that the extracted data is formatted appropriately for seamless integration into analytical workflows.

### Technical Requirements
The entire process is implemented and executed within Jupyter Notebook, leveraging Python for API interaction, data extraction, and transformation. The following tools and libraries are utilized:<br>
🔹requests – For sending API requests and retrieving JSON responses.<br>
🔹csv – For writing the extracted data into a CSV file.<br>
🔹uuid – For generating unique identifiers for each row.<br>
🔹pandas – For handling and structuring tabular data.<br>
🔹os – For checking the existence of saved files and managing file operations.

#### 1. Install Jupyter Notebook (if not installed)

```
pip install notebook
```
#### 2. Create a Dedicated Project Folder
Organize the work by creating a new project folder where to store the Jupyter Notebook and output files and navigate to this folder:
```
mkdir python_api_project
```
```
cd python_api_project
```
#### 3. Launch Jupyter Notebook
```
jupyter notebook
```
#### 4. Create a New Notebook<br>
🔹Click "New" → "Python 3 (ipykernel)" to create a new notebook.<br>
🔹Rename it to something meaningful, like api_data_transformation.ipynb.
#### 5. Write your python code.
#### 6. Save and Check Outputs<br>
🔹Verify that result_table.csv is saved in the same directory.<br>
🔹Use os.path.exists("result_table.csv") in a code cell to confirm file creation.
#### 7. Shut Down Jupyter Notebook When Done