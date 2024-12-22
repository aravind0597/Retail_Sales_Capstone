import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor

# Initialize Connection
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        database=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        port=st.secrets["postgres"]["port"]
    )

conn = init_connection()

# Perform Query
@st.cache_data(ttl=600)
def run_query(query):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)  # Execute the query
            return cur.fetchall()  # Fetch all results
    except Exception as e:
        # Rollback the transaction in case of an error
        conn.rollback()
        raise e
# Main Title
st.title("Retail Sales Analysis Dashboard")

# Section Heading
st.header("Explore Key Metrics")

st.subheader("Select the Questions below")
st.divider()

tab1,tab2 =  st.tabs(["Exiating Questions","New Questions"])
st.divider()

st.divider()
with tab1:
    question = st.selectbox(
        "Questions are:",
        [
            "Find top 10 highest revenue generating products",
            "Find the top 5 cities with the highest profit margins",
            "Calculate the total discount given for each category",
            "Find the average sale price per product category",
            "Find the region with the highest average sale price",
            "Find the total profit per category",
            "Identify the top 3 segments with the highest quantity of orders",
            "Determine the average discount percentage given per region",
            "Find the product category with the highest total profit",
            "Calculate the total revenue generated per year",
        ]
    )

    # Define Queries Based on the Selected Question
    if question == "Find top 10 highest revenue generating products":
        query = """
            SELECT "Product_id", SUM("Sale_price" * "Quantity") AS total_revenue
            FROM "retail_sales2"
            GROUP BY "Product_id"
            ORDER BY total_revenue DESC
            LIMIT 10;
        """
    elif question == "Find the top 5 cities with the highest profit margins":
        query = """
            SELECT "City", SUM("Profit") / SUM("Cost_price") AS profit_margin
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "City"
            ORDER BY profit_margin DESC
            LIMIT 5;
        """
    elif question == "Calculate the total discount given for each category":
        query = """
            SELECT "Category", SUM("Discount") AS total_discount
            FROM "retail_sales2" AS rs2
            JOIN "retail_sales1" AS rs1 ON rs2."Order_id" = rs1."Order_id"
            GROUP BY "Category"
            ORDER BY total_discount DESC;
        """
    elif question == "Find the average sale price per product category":
        query = """
            SELECT "Sub_category", AVG("Sale_price") AS avg_sale_price
            FROM "retail_sales2"
            GROUP BY "Sub_category"
            ORDER BY avg_sale_price DESC;
        """
    elif question == "Find the region with the highest average sale price":
        query = """
            SELECT "Region", AVG("Sale_price") AS avg_sale_price
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Region"
            ORDER BY avg_sale_price DESC
            LIMIT 1;
        """
    elif question == "Find the total profit per category":
        query = """
            SELECT "Category", SUM("Profit") AS total_profit
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Category"
            ORDER BY total_profit DESC;
        """
    elif question == "Identify the top 3 segments with the highest quantity of orders":
        query = """
            SELECT "Segment", SUM("Quantity") AS total_quantity
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Segment"
            ORDER BY total_quantity DESC
            LIMIT 3;
        """
    elif question == "Determine the average discount percentage given per region":
        query = """
            SELECT "Region", AVG("Discount_percent") AS avg_discount_percentage
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Region"
            ORDER BY avg_discount_percentage DESC;
        """
    elif question == "Find the product category with the highest total profit":
        query = """
            SELECT "Category", SUM("Profit") AS total_profit
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Category"
            ORDER BY total_profit DESC
            LIMIT 1;
        """
    elif question == "Calculate the total revenue generated per year":
        query = """
            SELECT EXTRACT(YEAR FROM TO_DATE("Order_date", 'YYYY-MM-DD')) AS year, 
                SUM("Sale_price" * "Quantity") AS total_revenue
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY year
            ORDER BY year;
        """
    else:
        query = None

    # Execute the Query and Display Results
    if query:
        try:
            rows = run_query(query)
            if rows:
                df = pd.DataFrame(rows)
                st.subheader(f"Results for: {question}")
                st.table(df)
            else:
                st.write("No data found.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.write("Please select a valid question.")
    
with tab2:
    # All queries mapped to questions
    queries = {
        "Find the top 3 products with the highest average discount percentage": """
            SELECT "Product_id", AVG("Discount_percent") AS avg_discount
            FROM "retail_sales2"
            GROUP BY "Product_id"
            ORDER BY avg_discount DESC
            LIMIT 3;
        """,
        "Find regions where the total revenue exceeds $1,000,000": """
            SELECT "Region", SUM("Sale_price" * "Quantity") AS total_revenue
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Region"
            HAVING SUM("Sale_price" * "Quantity") > 1000000
            ORDER BY total_revenue DESC;
        """,
        "Find the top 5 profitable products in each category": """
            WITH ranked_products AS (
                SELECT 
                    "Category",
                    "Product_id",
                    SUM("Profit") AS total_profit,
                    ROW_NUMBER() OVER (PARTITION BY "Category" ORDER BY SUM("Profit") DESC) AS rank
                FROM "retail_sales2" AS rs2
                JOIN "retail_sales1" AS rs1 ON rs2."Order_id" = rs1."Order_id"
                GROUP BY "Category", "Product_id"
            )
            SELECT "Category", "Product_id", total_profit
            FROM ranked_products
            WHERE rank <= 5;
        """,
        "Find categories with an average sale price above $500": """
            SELECT "Category", AVG("Sale_price") AS avg_sale_price
            FROM "retail_sales2" AS rs2
            JOIN "retail_sales1" AS rs1 ON rs2."Order_id" = rs1."Order_id"
            GROUP BY "Category"
            HAVING AVG("Sale_price") > 500
            ORDER BY avg_sale_price DESC;
        """,
        "Rank the top 5 cities with the highest quantity of orders": """
            SELECT 
                "City",
                SUM("Quantity") AS total_quantity,
                ROW_NUMBER() OVER (ORDER BY SUM("Quantity") DESC) AS rank
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "City"
            ORDER BY rank
            LIMIT 5;
        """,
        "Find the regions where the average discount percentage is greater than 20%": """
            SELECT "Region", AVG("Discount_percent") AS avg_discount
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Region"
            HAVING AVG("Discount_percent") > 20
            ORDER BY avg_discount DESC;
        """,
        "Rank the top 10 products by total revenue": """
            SELECT 
                "Product_id",
                SUM("Sale_price" * "Quantity") AS total_revenue,
                ROW_NUMBER() OVER (ORDER BY SUM("Sale_price" * "Quantity") DESC) AS rank
            FROM "retail_sales2"
            GROUP BY "Product_id"
            ORDER BY rank
            LIMIT 10;
        """,
        "Find the top 3 segments with the highest profit per order": """
            SELECT 
                "Segment",
                AVG("Profit") AS avg_profit_per_order,
                ROW_NUMBER() OVER (ORDER BY AVG("Profit") DESC) AS rank
            FROM "retail_sales1" AS rs1
            JOIN "retail_sales2" AS rs2 ON rs1."Order_id" = rs2."Order_id"
            GROUP BY "Segment"
            ORDER BY rank
            LIMIT 3;
        """,
        "Find subcategories where the total discount exceeds $10,000": """
            SELECT "Sub_category", SUM("Discount") AS total_discount
            FROM "retail_sales2"
            GROUP BY "Sub_category"
            HAVING SUM("Discount") > 10000
            ORDER BY total_discount DESC;
        """,
        "Rank the product categories by total profit": """
            SELECT 
                "Category",
                SUM("Profit") AS total_profit,
                ROW_NUMBER() OVER (ORDER BY SUM("Profit") DESC) AS rank
            FROM "retail_sales2" AS rs2
            JOIN "retail_sales1" AS rs1 ON rs2."Order_id" = rs1."Order_id"
            GROUP BY "Category"
            ORDER BY rank;
        """
    }
    
    selected_ques = st.selectbox("Select a Question",list(queries.keys()))

    if selected_ques:
        query = queries[selected_ques]
        st.subheader(f"Results for : {selected_ques}")
        rows = run_query(query)
        if rows:
            df1 = pd.DataFrame(rows)
            st.table(df1)
        else:
            st.write("No Data Found")
