import sys, os
import json

import PIL
from PIL import Image
from PIL.ExifTags import TAGS
#from PIL.TiffTags import TAGS as tiffTAGS
from PIL.ExifTags import GPSTAGS

# -- 20220620 add for GPSInfo --
from GPSPhoto import gpsphoto


imgFilePath = sys.argv[1]
imgFileName = os.path.basename(imgFilePath)
savePath = sys.argv[2]

def doSpecTuple(theTuple):
    # special case
    retStr = ''
    for i in range(len(theTuple)):
        val = theTuple[i]
        tmpStr = str(val)
        if len(retStr) == 0:
            retStr = '('+tmpStr
        else:
            retStr = retStr+', '+tmpStr
    # at last add '}'
    retStr = retStr+')'
    return retStr


def getExif(file_path):
    image = Image.open(file_path)
    exif = image._getexif()
    if exif is None:
        return
    exif_data = {}
    # -- 20220621 add filename into dict file at begin here --
    exif_data['filename'] = imgFileName
    
    try:
        for tag_id, value in exif.items():
            tag = TAGS.get(tag_id, tag_id)
            if not tag == "MakerNote" and not tag == "XPComment":
                print(f'{tag:25}: {value}')
            
            # GPS情報は個別に扱う．
            if tag == "GPSInfo":
                print('')
                gps_data = {}
                for t in value:
                    gps_tag = GPSTAGS.get(t, t)
                    content = value[t]
                    gps_data[gps_tag] = content
                    print(f'{gps_tag:25}: {content}')
                    # chk tuple or bytesvalue[t]
                    if isinstance(content, tuple):
                        #tupleStr = ','.join(map(str,content))
                        tupleStr = doSpecTuple(content)
                        exif_data[gps_tag] = tupleStr
                    elif isinstance(content, bytes):
                        if gps_tag == 'GPSVersionID' or gps_tag == 'GPSAltitudeRef' :
                            intVal = int.from_bytes(content, byteorder='big')
                            bytesStr = str(intVal)
                        else:
                            bytesStr = content.decode("utf-8")
                        exif_data[gps_tag] = bytesStr
                    elif isinstance(content, str):
                        if gps_tag == 'GPSMapDatum' or gps_tag == 'XPKeywords':
                            content = content.replace('\u0000','')
                        exif_data[gps_tag] = content
                    else:
                        exif_data[gps_tag] = str(content)
                
                print('')
                # -- do again GPSInfo for iso19115xml format --
                gps = gpsphoto.getGPSData(imgFilePath)
                long_lat = (gps['Latitude'], gps['Longitude'])
                exif_data['GPSInfo'] = long_lat
            else:
                if not tag == "MakerNote" and not tag == "XPComment":
                    # here handle for GPSInfo, chk tuple or bytes
                    if isinstance(value, tuple):
                        #tupleStr = ','.join(map(str,value))
                        tupleStr = doSpecTuple(value)
                        exif_data[tag] = tupleStr
                    elif isinstance(value, bytes):
                        if tag == 'XPKeywords':
                            bytesStr = value.decode("utf-8")
                            bytesStr = bytesStr.replace('\x00','')
                        elif tag == 'ComponentsConfiguration' or tag == 'DeviceSettingDescription' or tag == 'FileSource' or tag == 'SceneType':
                            intVal = int.from_bytes(value, byteorder='big')
                            bytesStr = str(intVal)
                        else:
                            bytesStr = value.decode("utf-8")
                        exif_data[tag] = bytesStr
                    elif isinstance(value, str):
                        exif_data[tag] = value
                    else:
                        exif_data[tag] = str(value)
                    
    except:
        pass

    return exif_data 


exif_res = getExif(imgFilePath)
#print(exif_res)

print()
print('------- flat dict is gerenrated ---------')
print()

theDictStr = json.dumps(exif_res)
saveFileName = imgFileName[:-4]+'.dict'		# we chg from .JPG to .dict -- 20220613
saveFileName = saveFileName.lower()
wf = open(savePath+'/'+saveFileName, "w")
wf.write(theDictStr)
wf.close()

print(savePath+'/'+saveFileName+' is created')
print('finish action')
