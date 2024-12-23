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
name_on_order = st.text_input('Name on Smoothie:' )
st.write('The name on your smoothie will be:', name_on_order)

# Connect to Snowflake
cnx = st.connection("snowflake")
session = cnx.session()

# Ensure the `order_filled` column exists
try:
    session.sql("""
        ALTER TABLE smoothies.public.orders ADD COLUMN order_filled BOOLEAN DEFAULT FALSE;
    """).collect()
except:
    st.warning("The 'order_filled' column already exists or couldn't be added.")

# Fetch fruit options from the database
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'), col('SEARCH_ON'))

# Convert the Snowpark DataFrame to a Pandas DataFrame
pd_df = my_dataframe.to_pandas()
st.dataframe(pd_df)

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

# Add predefined orders for Kevin, Divya, and Xi
if st.button('Create Predefined Orders'):
    try:
        # Kevin's order
        session.sql("""
            INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
            VALUES ('Apples, Lime, Ximenia', 'Kevin', FALSE);
        """).collect()
        
        # Divya's order
        session.sql("""
            INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
            VALUES ('Dragon Fruit, Guava, Figs, Jackfruit, Blueberries', 'Divya', TRUE);
        """).collect()
        
        # Xi's order
        session.sql("""
            INSERT INTO smoothies.public.orders (ingredients, name_on_order, order_filled)
            VALUES ('Vanilla Fruit, Nectarine', 'Xi', TRUE);
        """).collect()
        
        st.success("Predefined orders for Kevin, Divya, and Xi have been created!", icon="✅")
    except Exception as e:
        st.error(f"Failed to create predefined orders: {e}")
