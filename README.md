# sample-flipkart-data-etl

**Project Description**
This project focuses on analyzing Flipkart's product sales data by transforming raw data into a structured format suitable for insights and decision-making. The primary goal is to identify trends in pricing, discounts, and sales performance based on product categories.

**Data Cleaning & Transformation**

**Dropped Columns**
>> Some columns, such as brand, were dropped due to inconsistent or missing values that did not contribute meaningfully to the analysis.
>> Other irrelevant attributes that did not impact sales trends were removed to enhance data quality and improve processing efficiency.

**Product Category Tree Transformation**
>> The product_category_tree column originally contained nested and hierarchical product data.
>> It was transformed into a simplified "category" column, making it easier to analyze and classify products.
>> This ensures better categorization and helps in understanding product trends effectively.

**Objectives**
>> Analyze the highest sale prices based on product categories.
>> Identify pricing trends across different categories.
>> Ensure data consistency for effective visualization and insights.
