def get_ct_queries():
    accounts_ct = """
    create table if not exists accounts_dim (
        customer_id integer primary key not null,
        first_name varchar,
        last_name varchar,
        address_1 varchar,
        address_2 varchar,
        city varchar,
        state varchar,
        zip_code integer,
        join_date date
    )
    """
    products_ct = """ 
    create table if not exists products_dim (
        product_id integer primary key not null,
        product_code varchar,
        product_description varchar
        )
    """
    transactions_ct = """
    create table if not exists transactions_fct (
        transaction_id varchar primary key not null, 
        transaction_date date, 
        product_id integer , 
        product_code varchar, 
        product_description varchar, 
        quantity integer, 
        account_id integer,
        constraint fk_product foreign key (product_id) references products_dim(product_id),
        constraint fk_account foreign key (account_id) references accounts_dim(customer_id)
    )
    """
    return [
        accounts_ct,
        products_ct,
        transactions_ct,
    ]


def get_dt_query(table_name):
    return f"drop table if exists {table_name} cascade"


def get_index_query(table_name, column):
    return f"create index {table_name}_idx on {table_name}({column});"
    
