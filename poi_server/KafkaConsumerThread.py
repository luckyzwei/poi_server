# coding: utf-8
import traceback
from public.logger import simple_log

logger = simple_log()
class KafkaConsumerThread(object):
    def judge_ifPoi_in_city_scope(self, f_poi_lng, f_poi_lat, right_bottom_lng, right_bottom_lat, left_up_lng,
                                  left_up_lat):
        """
        :param f_poi_lng:
        :param f_poi_lat:
        :param right_bottom_lng:
        :param right_bottom_lat:
        :param left_up_lng:
        :param left_up_lat:
        :return:
        """
        try:
            leftUpLat = left_up_lat + 180.0;
            leftUpLng = left_up_lng + 360.0;
            rightBottomLat = right_bottom_lat + 180.0;
            rightBottomLng = right_bottom_lng + 360.0;
            minLat = min(leftUpLat, rightBottomLat);
            minLng = min(leftUpLng, rightBottomLng);
            maxLat = max(leftUpLat, rightBottomLat);
            maxLng = max(leftUpLng, rightBottomLng);
            fPoiLat = f_poi_lat + 180.0;
            fPoiLng = f_poi_lng + 360.0;
            if fPoiLat > minLat and fPoiLat < maxLat and fPoiLng > minLng and fPoiLng < maxLng:
                return True;
            return False
        except:
            logger.error(traceback.format_exc())
            return False
