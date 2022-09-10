import os,sys
from PIL import Image
from PIL.ExifTags import TAGS
#import cv2
import glob
from xml.dom.minidom import Document
import os.path
from os.path import exists
import xml.etree.ElementTree as ET
from GPSPhoto import gpsphoto

#read image file from folder
#imagepath = glob.glob("/Users/regita/Documents/pythoncode/3dimages/*.jpg")
# 
if len(sys.argv) >= 3:
    GXdataFile = sys.argv[1]
    saveXmlPath = sys.argv[2]
elif len(sys.argv) == 2:
    GXdataFile = sys.argv[1]
    saveXmlPath = os.getcwd()
else:
    chkFlag = False

#imagepath = glob.glob("./*.JPG")
extrachtDict = {}

# read the image data using PIL
#for imagefile in imagepath:
if exists(GXdataFile):
    image = Image.open(GXdataFile)
    #name = imagefile[44:-4]
    img_dict = image._getexif().items()
    #make xml scema for metadata
    root = Document()

    mdb = root.createElementNS("https://schemas.isotc211.org/19115/-1/mdb/1.3.0", "mdb:MD_Metadata")
    mdb.setAttribute("xmlns:mdb", "https://schemas.isotc211.org/19115/-1/mdb/1.3.0")
    mdb.setAttribute("xmlns:gco", "https://schemas.isotc211.org/19103/-/gco/1.2.0")
    mdb.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink")
    mdb.setAttribute("xmlns:gml", "http://www.opengis.net/gml")
    mdb.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance") 
    mdb.setAttribute("xsi:schemaLocation", "http://www.isotc211.org/2000/mdb http://www.isotc211.org/2005/mdb/metadataEntity.xsd") 
    root.appendChild(mdb)
    
    hierarchy1_parent1 = root.createElement('mdb:fileIdentifier')
    mdb.appendChild(hierarchy1_parent1)
    filename = root.createTextNode(GXdataFile)
    hierarchy1_parent1.appendChild(filename)

    hierarchy2_parent1 = root.createElement('mdb:characterSet')
    mdb.appendChild(hierarchy2_parent1)
    hierarchy2_child1 = root.createElement('mdb:MD_CharacterSetCode')
    hierarchy2_parent1.appendChild(hierarchy2_child1)
    hierarchy2_child1.setAttribute('codeList', 'http://www.isotc211.org/2000/resources/codeList.xml#MD_CharacterSetCode')
    hierarchy2_child1.setAttribute('codeListValue', 'utf-8') 
    hierarchy2_child1.setAttribute('codeSpace', 'ISOTC211/19115')
    utf8 = root.createTextNode('utf-8')
    hierarchy2_child1.appendChild(utf8)

    hierarchy3_parent1 = root.createElement('mdb:dateStamp')
    mdb.appendChild(hierarchy3_parent1)
    hierarchy3_child1 = root.createElement('gco:DateTime')
    hierarchy3_parent1.appendChild(hierarchy3_child1)
    hierarchy3_child2 = root.createElement('DateTimeOriginal')
    hierarchy3_parent1.appendChild(hierarchy3_child2)
    hierarchy3_child3 = root.createElement('DateTimeDigitized')
    hierarchy3_parent1.appendChild(hierarchy3_child3)

    hierarchy4_parent1 = root.createElement('mdb:spatialRepresentationInfo')
    mdb.appendChild(hierarchy4_parent1)
    hierarchy4_parent1.setAttribute('xlink:type', 'simple') 
    hierarchy4_parent2 = root.createElement('gml:coordinates')
    hierarchy4_parent1.appendChild(hierarchy4_parent2)
    hierarchy4_child1 = root.createElement('gml:Point')
    hierarchy4_parent2.appendChild(hierarchy4_child1)

    hierarchy5_parent1 = root.createElement('mdb:identificationInfo')
    mdb.appendChild(hierarchy5_parent1)
    hierarchy5_parent1.setAttribute('xlink:type', 'simple') 
    hierarchy5_parent2 = root.createElement('mdb:MD_DataIdentification')
    hierarchy5_parent1.appendChild(hierarchy5_parent2)
    hierarchy5_child1 = root.createElement('gml:metaDataProperty')
    hierarchy5_parent2.appendChild(hierarchy5_child1)
    hierarchy5_child2 = root.createElement('mdb:metadataExtensionInfo')
    hierarchy5_child1.appendChild(hierarchy5_child2)

    hierarchy5_child3 = root.createElement('mdb:spatialResolution')
    hierarchy5_parent2.appendChild(hierarchy5_child3)
    hierarchy5_child4 = root.createElement('mdb:MD_Resolution')
    hierarchy5_child3.appendChild(hierarchy5_child4)
    hierarchy5_child5 = root.createElement('XResolution')
    hierarchy5_child4.appendChild(hierarchy5_child5)
    hierarchy5_child6 = root.createElement('YResolution')
    hierarchy5_child4.appendChild(hierarchy5_child6)
    hierarchy5_child7 = root.createElement('ResolutionUnit')
    hierarchy5_child4.appendChild(hierarchy5_child7)
    hierarchy5_child8 = root.createElement('ImageWidth')
    hierarchy5_child4.appendChild(hierarchy5_child8)
    hierarchy5_child9 = root.createElement('ImageLength')
    hierarchy5_child4.appendChild(hierarchy5_child9)

    gps = gpsphoto.getGPSData(GXdataFile)
    long_lat = (gps['Latitude'], gps['Longitude'])

    # print(root.toprettyxml(indent = '\t'))
    for k, v in img_dict :
        tag = TAGS.get(k, k)
        value = str(v)

        if tag not in ['GPSInfo', 'DateTime', 'DateTimeOriginal', 'DateTimeDigitized', 'XResolution', 'YResolution', 'ResolutionUnit', 'ImageWidth', 'ImageLength']:
            ExtensionInfo = root.createElement(tag)
            hierarchy5_child2.appendChild(ExtensionInfo)
            ExtensionInfotext = root.createTextNode(value)
            ExtensionInfo.appendChild(ExtensionInfotext)

        if tag == 'GPSInfo':
            long_lat = str(long_lat)
            gpsvalue = root.createTextNode(long_lat)
            hierarchy4_child1.appendChild(gpsvalue)

        if tag == 'DateTime':
            value = value.replace(" ", "T")
            datetime = root.createTextNode(value)
            hierarchy3_child1.appendChild(datetime)

        if tag == 'DateTimeOriginal':
            value = value.replace(" ", "T")
            dateoriginal = root.createTextNode(value)
            hierarchy3_child2.appendChild(dateoriginal)

        if tag == 'DateTimeDigitized':
            value = value.replace(" ", "T")
            datedigitize = root.createTextNode(value)
            hierarchy3_child3.appendChild(datedigitize)
        
        if tag == 'XResolution':
            xres = root.createTextNode(value)
            hierarchy5_child5.appendChild(xres)

        if tag == 'YResolution':
            yres = root.createTextNode(value)
            hierarchy5_child6.appendChild(yres)

        if tag == 'ResolutionUnit':
            ures = root.createTextNode(value)
            hierarchy5_child7.appendChild(ures)

        if tag == 'ImageWidth':
            Iwidth = root.createTextNode(value)
            hierarchy5_child8.appendChild(Iwidth)

        if tag == 'ImageLength':
            Ilength = root.createTextNode(value)
            hierarchy5_child9.appendChild(Ilength)

    
    xml_str = root.toprettyxml(indent = '\t')
    # print(xml_str)
    fileName, fileExt = os.path.splitext(GXdataFile)
    #save_path_file = '/Users/regita/Documents/pythoncode/xml'
    #file_name = fileName.lower() + ".xml"
    file_name = fileName + ".xml"
    completeName = os.path.join(saveXmlPath, file_name)
  
    file1 = open(completeName, "w")
    file1.write(xml_str)
    file1.close()
else:
    print('sorry! file not found')







