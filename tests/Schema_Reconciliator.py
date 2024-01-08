import csv
class SchameReconciliator(object):

    #TODO: Before running script provide schema locations
    other_schema_loc= None
    base_schema_loc= None


    C_CLM_NAME_HEADER= 'column'
    C_TBL_NME_HEADER= 'table_name'
    C_SCHEMA_HEADER= 'schemaname'
    C_DATA_TYPE_HEADER= 'data_type'

    def reconcile(self):
        if (self.other_schema_loc is None)or (self.base_schema_loc is None):
            print("Please provide both schema locations")
            return

        print("Reconciliation started")

        base_schema_map,base_schme_name = self.__parse_csv(self.base_schema_loc)

        other_schema_map,other_schema_name = self.__parse_csv(self.other_schema_loc)
        print("Starting reconciliation process...")
        table_missing_list = []
        column_missing_list = []
        column_schema_mismatch_list = []
        for table,base_tbl_details in base_schema_map.items():

            other_tbl_details= other_schema_map.get(table)

            if other_tbl_details:
                for clm, data_type in base_tbl_details.items():
                    other_clm_type = other_tbl_details.get(clm)
                    if other_clm_type is None:
                        column_missing_list.append("Column {} in {} table is missing in {} schema".format(clm,table, other_schema_name))
                    elif data_type!= other_clm_type:
                        msg = "Data type of Column {} in {} is not same, {} has {}, {} has {}".format(clm,table
                                                                                                      ,base_schme_name,data_type, other_schema_name,other_clm_type)
                        column_schema_mismatch_list.append(msg)
            else:
                table_missing_list.append(table)

        print("***********************************************************")
        print("List of missing table from schema {}".format(other_schema_name))
        for table in table_missing_list:
            print(table)
        print("***********************************************************\r\n")

        print("***********************************************************")
        print("List of missing columns from schema {}".format(other_schema_name))
        for clm_missing_msg in column_missing_list:
            print(clm_missing_msg)
        print("***********************************************************\r\n")


        print("***********************************************************")
        print("List of mismatch data type in columns")
        for clm_missing_msg in column_schema_mismatch_list:
            print(clm_missing_msg)
        print("***********************************************************")

        print("Reconciliation process completed...")

    def __parse_csv(self, file_loc):
        print("Reading schema CSV {}".format(file_loc))
        table_column_list= {}
        schema_name= None
        with open(file_loc) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter='|')
            for line in csv_reader:
                table_name = line[self.C_TBL_NME_HEADER]
                if schema_name is None:
                    schema_name = line[self.C_SCHEMA_HEADER]
                    print("Found schema {}".format( line[self.C_SCHEMA_HEADER]))
                clm_info_map =  table_column_list.get(table_name)
                if clm_info_map is  None:
                    clm_info_map = {}
                    table_column_list[table_name]=clm_info_map

                clm_info_map[line[self.C_CLM_NAME_HEADER]] = line[self.C_DATA_TYPE_HEADER]
        print("Schema file read successfully")
        return table_column_list, schema_name



def main():
    job = SchameReconciliator()
    job.reconcile()


if __name__ == '__main__':
    main()
