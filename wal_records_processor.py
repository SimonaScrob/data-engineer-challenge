import json

from sqlite_client import SQLiteClient
from constants import event_v2_data, transaction, transaction_request, payment_instrument_token_data, \
    event_v2_data_column_names, transaction_column_names, transaction_request_column_names, \
    payment_instrument_token_data_column_names, db_name, new_table, nested_key_mapping, file_name
from utils import search_in_nested_dict, validate_input


class WalRecordsProcessor:
    """Class used for processing WAL records. """

    def __init__(self):
        self.event_v2_data_dict = {}
        self.transaction_dict = {}
        self.transaction_request_dict = {}
        self.payment_instrument_token_data_dict = {}

        # set default type None for all columns
        self.column_names_types_mapping = {
            event_v2_data: {name: None for name in event_v2_data_column_names},
            transaction: {name: None for name in transaction_column_names},
            transaction_request: {name: None for name in transaction_request_column_names},
            payment_instrument_token_data: {name: None for name in payment_instrument_token_data_column_names}
        }

    def get_subset(self, change):
        """The method takes the column_name, column_value and column_type from the given record, builds a subset with
        column names and values and updates the column type in column_names_types_mapping.
        :param change
                type: dict
                Format: {
                        "kind": "insert",
                        "schema": "public",
                        "table": "foo",
                        "columnnames": ["a", "b", "c"],
                        "columntypes": ["integer", "character varying(30)", "timestamp without time zone"],
                        "columnvalues": [1, "Backup and Restore", "2018-03-27 12:05:29.914496"]
                      }
        :return: dict
             {  a: 1,
                c: "2018-03-27 12:05:29.914496"
             } where a and c are columns of table "foo" that should be added in the final subset
        """
        columns_values_subset = {}  # {<column_name>: <column_value>}
        table_name = change["table"]
        # iterate over all column names (of the current table) that should be added in the final subset
        for column_name in self.column_names_types_mapping[table_name]:
            # check if column name exists in record's column names
            try:
                index = change["columnnames"].index(column_name)
                # if the column name was found, add the <column_name> <column_value> in the dictionary
                columns_values_subset[column_name] = change["columnvalues"][index]
                # update the type if it was not added yet
                if not self.column_names_types_mapping[table_name][column_name]:
                    self.column_names_types_mapping[table_name][column_name] = change["columntypes"][index]
            except ValueError:
                # if the column name was not found in record's column names, it might be found in nested values
                value = None
                if column_name in nested_key_mapping:  # nested_key_mapping = {<column_name>: <parent>}
                    try:
                        parent = nested_key_mapping[column_name]
                        # check if the parent key exists in record's column names
                        index = change["columnnames"].index(parent)
                        if change["columntypes"][index] == "jsonb" and change["columnvalues"][index]:
                            nested_field = json.loads(change["columnvalues"][index])
                            value = search_in_nested_dict(nested_field, column_name)
                    except (ValueError, json.JSONDecodeError) as e:
                        raise e
                columns_values_subset[column_name] = value
                # update the type if it was not added yet
                if not self.column_names_types_mapping[table_name][column_name]:
                    self.column_names_types_mapping[table_name][column_name] = "character"
        return columns_values_subset

    def read_records(self):
        """
        Reads the WAL records from file and store the subsets of the records in 4 dictionaries (one for each table).
        """
        try:
            # Read the WAL records
            with open(file_name, "r") as f:
                records = json.loads(f.read())
                for record in records:
                    validate_input(record)

                    change = record["change"][0]
                    table_name = change["table"]

                    columns_values_subset = self.get_subset(change)

                    # add the record's subset in the dictionary that corresponds to the table name
                    if table_name == event_v2_data:
                        transaction_id = columns_values_subset.get("transaction_id")
                        flow_id = columns_values_subset.get("flow_id")

                        if transaction_id and flow_id:
                            if (transaction_id, flow_id) not in self.event_v2_data_dict.keys():
                                self.event_v2_data_dict[(transaction_id, flow_id)] = [columns_values_subset]
                            else:
                                self.event_v2_data_dict[(transaction_id, flow_id)].append(columns_values_subset)

                    elif table_name == transaction:
                        transaction_id = columns_values_subset.get("transaction_id")
                        if transaction_id:
                            self.transaction_dict[transaction_id] = columns_values_subset

                    elif table_name == transaction_request:
                        flow_id = columns_values_subset.pop("flow_id")
                        if flow_id:
                            self.transaction_request_dict[flow_id] = columns_values_subset

                    elif table_name == payment_instrument_token_data:
                        token_id = columns_values_subset.pop("token_id")
                        if token_id:
                            self.payment_instrument_token_data_dict[token_id] = columns_values_subset

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            print("Wrong format of input data")
        except FileNotFoundError:
            print("Input file not found")

    def __create_table(self, sql_client, new_table):
        """Creates a new table in sqlite database.
        :param sql_client: SQLiteClient
        :param new_table: str
        """
        # merge the dictionaries which contains the <column_name>: <column_type> mapping
        all_column_name_type_mapping = {
            **self.column_names_types_mapping[event_v2_data],
            **self.column_names_types_mapping[transaction],
            **self.column_names_types_mapping[transaction_request],
            **self.column_names_types_mapping[payment_instrument_token_data]
        }
        all_column_name_type_mapping.pop("token_id")  # token_id should not appear in the final subset

        sql_client.create_table(new_table, **all_column_name_type_mapping)

    def run(self):
        """Joins the tables and inserts the subsets in the database.
        """
        self.read_records()
        with SQLiteClient(db_name) as sql_client:
            self.__create_table(sql_client, new_table)
            # iterate over event_v2_data_dict items
            for (transaction_id, flow_id), events_column_names_values in self.event_v2_data_dict.items():
                # join the tables
                if transaction_id in self.transaction_dict and flow_id in self.transaction_request_dict and \
                        self.transaction_request_dict[flow_id].get(
                            "token_id") in self.payment_instrument_token_data_dict:
                    # each value of event_v2_data_dict is a list of event_v2_data subset
                    for event_column_names_values in events_column_names_values:
                        subset_result = {}
                        # add required column names and values from event_v2_data
                        subset_result.update(event_column_names_values)
                        token_id = self.transaction_request_dict[flow_id].get("token_id")
                        # add required columns and values from transaction
                        subset_result.update(self.transaction_dict[transaction_id])
                        # add required columns and values from transaction_request
                        subset_result.update(self.transaction_request_dict[flow_id])
                        # add required columns and values from payment_instrument_token_data_dict
                        subset_result.update(self.payment_instrument_token_data_dict[token_id])
                        # remove token_id from the final subset
                        subset_result.pop("token_id")
                        # insert subset in db
                        sql_client.insert_row(new_table, **subset_result)
