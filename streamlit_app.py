# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """Choose the fruits you want in the custom Smoothie!
    """
)

# Input for customer name
name_on_order = st.text_input('Name on Smoothie:')
st.write('The name on your smoothie will be:', name_on_order)

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Ensure the order_filled column exists
try:
    # Query to check if the column 'order_filled' exists
    check_column_query = """
        SELECT * FROM information_schema.columns 
        WHERE table_name = 'ORDERS' 
        AND column_name = 'ORDER_FILLED';
    """
    
    # Run the query
    check_column_result = session.sql(check_column_query).collect()

    # If the column is not found, add it
    if not check_column_result:
        session.sql("""
            ALTER TABLE smoothies.public.orders 
            ADD COLUMN order_filled BOOLEAN DEFAULT FALSE;
        """).collect()
        st.success("Column 'order_filled' was successfully added.")
    else:
        st.warning("The 'order_filled' column already exists.")

except Exception as e:
    st.error(f"Error while checking or adding 'order_filled' column: {e}")

# Fetch fruit options from the database
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert the Snowpark DataFrame to a Pandas DataFrame
pd_df = my_dataframe.to_pandas()
st.dataframe(pd_df)  # Display the fruit options

# Select ingredients
ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:',
    pd_df['FRUIT_NAME'].tolist()
)

if ingredients_list:
    # Build the ingredients string
    ingredients_string = ', '.join(ingredients_list)

    # Display search values and fetch nutrition info for each fruit
    for fruit_chosen in ingredients_list:
        search_on = pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write(f"The search value for {fruit_chosen} is {search_on}.")
        
        # Fetch nutrition information
        st.subheader(f"{fruit_chosen} Nutrition Information")
        smoothiefroot_response = requests.get(f"https://my.smoothiefroot.com/api/fruit/{search_on}")
        if smoothiefroot_response.status_code == 200:
            sf_df = pd.DataFrame(smoothiefroot_response.json())
            st.dataframe(sf_df, use_container_width=True)
        else:
            st.error(f"Failed to fetch nutrition information for {fruit_chosen}.")

    # Option to mark order as filled
    order_filled = st.checkbox('Mark order as filled', value=False)

    # Construct the SQL INSERT statement
    my_insert_stmt = f"""
        INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
        VALUES ('{ingredients_string}', '{name_on_order}', {order_filled});
    """

    # Button to submit the order
    time_to_insert = st.button('Submit Order')
    
    if time_to_insert:
        try:
            session.sql(my_insert_stmt).collect()
            st.success(f"Your Smoothie is ordered, {name_on_order}!", icon="✅")
        except Exception as e:
            st.error(f"Failed to submit the order: {e}")

# Retrieve some orders from the database to display them
query = "SELECT * FROM smoothies.public.orders LIMIT 5;"
orders_result = session.sql(query).collect()

# Convert the result into a pandas DataFrame and display it
if orders_result:
    orders_df = pd.DataFrame([dict(row) for row in orders_result])  # Convert Rows to dict
    st.dataframe(orders_df)  # Display the dataframe in Streamlit
else:
    st.warning("No orders found.")
