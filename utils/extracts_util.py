import sys


class BaseConfigValidator(object):

    def validate(self, p_config_dict, p_config_list):
        ErrorMsg = "|"
        for key,val in p_config_dict.items():
            if str(val).strip() == "":
                ErrorMsg = ErrorMsg+"{} cannot be left empty".format(key)+"|"

        l_missing_list = list(set(p_config_list) - set(p_config_dict.keys()))
        if l_missing_list.__len__()!=0 :
            ErrorMsg = ErrorMsg+ "Mandatory properties missing - {}".format(l_missing_list)
        if ErrorMsg != "|":
            print("Error - {}".format(ErrorMsg))
        else:
            print("All properties are validated")

        return ErrorMsg


class MetadataConfigValidator(BaseConfigValidator):

    def validate(self, p_config_dict, p_config_list):
        ErrorMsg = "|"
        for key,val in p_config_dict.items():
            if (str(key) != "input.header.list") & (str(val).strip() == ""):
                ErrorMsg += "{} cannot be left empty".format(key)+"|"
            elif (str(key) == "input.header.list") & (str(val).strip() == "") & (str(p_config_dict["input.header.present"]).lower() == "false"):
                ErrorMsg += "{} cannot be left empty if header.present is False".format(key)

        l_missing_list = list(set(p_config_list) - set(p_config_dict.keys()))
        if l_missing_list.__len__()!=0 :
            ErrorMsg = ErrorMsg+ "Mandatory properties missing - {}".format(l_missing_list)

        if ErrorMsg != "|":
            print("Error - {}".format(ErrorMsg))
        else:
            print("All properties are validated")

        return ErrorMsg


class CommonConfigValidator(BaseConfigValidator):
    pass

def main():
    import configparser
    config_path ="../configs/extracts_config/new_account_config.txt"
    config = configparser.RawConfigParser()
    config.read(config_path)

    g_metadata_config_dict = dict(config.items("metadata"))
    g_metadata_prop_list = ['input.location', 'header.present', 'header.list', 'output.location', 'output.file.name', 'separator']

    g_is_metadata_valid = MetadataConfigValidator().validate(g_metadata_config_dict,g_metadata_prop_list)

    if g_is_metadata_valid:
        g_date_config_dict = dict(config.items("date"))
        g_date_prop_list = ['format.date.input', 'format.date.output', 'column.date.list']
        g_is_date_valid = CommonConfigValidator().validate(g_date_config_dict,g_date_prop_list)

if __name__ == "__main__":
    main()