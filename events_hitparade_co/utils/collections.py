class CollectionsUtil:
    @staticmethod
    def SP(dict_to_set={}, prop_base=None, prop_name=None, separator='.', prop_val=None):
        if prop_base and prop_val:
            if prop_name:
                prop_key = prop_base + separator + prop_name
                dict_to_set[prop_key] = prop_val
            else:
                dict_to_set[prop_base] = prop_val
        return dict_to_set

    @staticmethod
    def GP(dict_to_get={}, prop_base=None, prop_name=None, separator='.', default_value=None):
        if prop_base:
            if prop_name:
                new_key = prop_base + separator + prop_name
                v1 = dict_to_get.get(new_key, None)
                if not v1:
                    v1 = dict_to_get.get(prop_base, None)
                    if v1:
                        return v1.get(prop_name, default_value)
                    else:
                        return default_value
                else:
                    return v1

            else:
                return dict_to_get.get(prop_base, default_value)
        else:
            return default_value
