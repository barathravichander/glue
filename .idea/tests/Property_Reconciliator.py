import os.path
import unittest


## This class will read All property from property_constant.py and
# validate that its value is present in s3_config or glue_catalog_config

class PropertyReconciliator(unittest.TestCase):
    C_GLUE_PROP_LOC = "../configs/glue_catalog_config.properties"
    C_S3_PROP_LOC = "../configs/s3_config.properties"
    C_PYTHON_SRC_LOC = "../constant/property_constant.py"

    C_KNOWN_MISMATCH_PROP_KEYS = ['FILE_TYPE', 'RUN_VALIDATION', 'BATCH_DATE', 'CLIENT_NUMBER', 'RECORD_TYPE', 'ENV', 'SOURCE_SYSTEM_NM',
                                  '[glue_catalog_config.properties, s3_config.properties,env.properties]', 's3.{}.stg.output.location',

                                  's3.{}.stg.output.location', 's3.{}.stg.excp.output.location', 'glue.s3.table.{}', 'PROP', 'SOURCE_SYSTEM_NM',"BATCH_TYPE"]


    @classmethod
    def setUpClass(clz):
        current_wrk_path = os.path.abspath(os.path.dirname(__file__))
        clz.python_props_consta_key, clz.python_props_constant_value = PropertyReconciliator.__read_file(current_wrk_path, clz.C_PYTHON_SRC_LOC, True)
        clz.s3_properties_key, clz.s3_properties_value = PropertyReconciliator.__read_file(current_wrk_path, clz.C_S3_PROP_LOC, False)
        clz.glue_properties_key, clz.glue_properties_value = PropertyReconciliator.__read_file(current_wrk_path, clz.C_GLUE_PROP_LOC, False)
        print("Test case setup is completed")
        print("\r\n")

    def test_property_mismatch(self):
        print("************************************")
        print("Starting Properties reconciliation..")
        print("************************************")
        prop_not_present = []
        for prop in self.python_props_constant_value:

            if prop in self.s3_properties_key:
                continue
            if prop in self.glue_properties_key:
                continue
            if prop in self.C_KNOWN_MISMATCH_PROP_KEYS:
                continue
            prop_not_present.append(prop)

        if len(prop_not_present) == 0:
            print("All property reconcile successfully\r\n")
        else:
            # print("\r\n----------------------------------------------------------------------")
            print("Mismatch found in property found...")
            for prop in prop_not_present:
                print(prop)
            # print("----------------------------------------------------------------------\r\n")
        self.assertTrue(len(prop_not_present) == 0, 'Property Mismatch found')

    def test_s3_property_duplicate(self):

        print("\r\nValidating duplicate values in s3_config")
        duplicate_found = PropertyReconciliator.__validate_for_duplicate(self.s3_properties_value, "S3_config")
        self.assertFalse(duplicate_found, 'S3 configuration property has duplicates')

    def test_glue_catalog_property_duplicate(self):
        print("\r\nValidating duplicate values in glue_catalog")
        duplicate_found = PropertyReconciliator.__validate_for_duplicate(self.glue_properties_value, "Glue Catalog")
        self.assertFalse(duplicate_found, 'Glue catalog configuration property has duplicates')

    @staticmethod
    def __validate_for_duplicate(properties, prop_name):
        duplicate_found = False
        glue_Catlog_dupes = PropertyReconciliator.__duplicate_finder(properties)
        if len(glue_Catlog_dupes) == 0:
            print("Duplicate not found in {}".format(prop_name))
        else:
            print("----------------------------------------------------------------------")
            print("Duplicate found in {}..".format(prop_name))
            for val in glue_Catlog_dupes:
                print(val)
            print("----------------------------------------------------------------------\r\n")
            duplicate_found = True
        return duplicate_found

    @staticmethod
    def __duplicate_finder(val_list):
        seen = {}
        dupes = []
        for x in val_list:
            if x not in seen:
                seen[x] = 1
            else:
                if seen[x] == 1:
                    dupes.append(x)
                seen[x] += 1
        return dupes

    @staticmethod
    def __read_file(current_wrk_path, file_path, pick_key=True):
        print(" Reading file {}".format(file_path))
        glue_prop_path = os.path.join(current_wrk_path, file_path)
        with open(glue_prop_path) as f:
            content = f.readlines()
            glue_property_key, glue_property_value = PropertyReconciliator.__read_property(content, pick_key)
        print("Completed reading file {}".format(file_path))
        return glue_property_key, glue_property_value

    @staticmethod
    def __read_property(content, pick_key=True):
        property_keys = []
        property_values = []
        for x in content:
            x = x.strip()
            if x.__contains__("#"):
                # print("skipping comment line {}".format(x))
                continue

            if len(x) == 0:
                # print("Empty line..")
                continue
            # to skip [property] line from se and glue catalog prop
            p = x.replace("\"", "").replace("\'", "").split("=")

            if len(p) != 2:
                # print("Skip line.")
                continue

            property_keys.append(p[0].strip())
            property_values.append(p[1].strip())
        return property_keys, property_values



if __name__ == '__main__':
    unittest.main()
