

KLAB_GEO_PROJ = "EPSG:4326"
KLAB_UNRESOLVED_OBS_ID = -1


class BaseModel:
    def to_dict(self):
        result = {}
        for key, value in self.__dict__.items():
            if hasattr(value, "to_dict"):
                result[key] = value.to_dict()
            elif isinstance(value, list):
                result[key] = [
                    item.to_dict() if hasattr(item, "to_dict") else item
                    for item in value
                ]
            else:
                result[key] = value
        return result
