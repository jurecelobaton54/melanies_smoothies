# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col, when_matched
import requests

# Write directly to the app
st.title(":cup_with_straw: Customize Your Smoothie! :cup_with_straw:")
st.write(
    """
    Choose the fruits you want in your custom Smoothie!
    """
)

name_on_order = st.text_input("Name on Smoothie: ")
st.write("The name on your Smoothie will be: ", name_on_order)


# option = st.selectbox(
#     "What is your favorite fruit?",
#     ("Banana", "Strawberries", "Peaches"),
# )

# st.write("You selected:", option)

# Changes made for converting SiS application into SniS application.
cnx = st.connection("snowflake")
session = cnx.session()

# session = get_active_session()

my_dataframe = session.table("smoothies.public.fruit_options").select(col('fruit_name'), col('search_on'))
# st.dataframe(data=my_dataframe, use_container_width=True)
# st.stop()

# Convert the Snowpark Dataframe into a Pandas Dataframe so we can use the LOC function
pd_df = my_dataframe.to_pandas()
# st.dataframe(pd_df)
# st.stop()


ingredients_list = st.multiselect(
                    'Choose up to 5 ingredients:',
                    my_dataframe,
                    max_selections = 5
)

if ingredients_list:
    # st.write(ingredients_list)
    # st.text(ingredients_list)

    ingredients_string = ''

    for fruit_chosen in ingredients_list:
        ingredients_string += fruit_chosen + ' '

        
        search_on=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', search_on, '.')

        
        # New section to display smoothiefront nutrition information
        
        # This is the original line of code which the workshop is asking me to use.
        # It seems that this API provider's website as not available.
        # smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/watermelon")
        
        # As a workaround I have used fruityvice.com as my data provider.

        st.subheader(fruit_chosen + ' Nutrition Information')
                
        smoothiefroot_response = requests.get("https://fruityvice.com/api/fruit/" + search_on)
        sf_df = st.dataframe(data = smoothiefroot_response.json(), use_container_width = True)
    
        # st.write(ingredients_string)


    my_insert_stmt = """ 
                        insert into smoothies.public.orders(ingredients, name_on_order)
                        values ('""" + ingredients_string + """', '""" + name_on_order + """')
                     """
    # st.write(my_insert_stmt)
    # st.stop()

    
    time_to_insert = st.button('Submit Order')


    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered, ' + name_on_order + '!', icon="✅")
