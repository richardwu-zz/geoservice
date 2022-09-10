import json
from xml.dom import minidom, Node
import sys, os

dictFilePath = sys.argv[1]
dictFileName = sys.argv[2]
fileName = dictFileName[:-5]     # len('.dict') = 5
print(fileName)
# without 'DateTime' in EXIF, we cancel it!
usefullItems = ['GPSLatitude', 'GPSLongitude', 'XResolution', 'YResolution', 'ExifImageWidth', 'ExifImageHeight', 'DateTime']

doc = minidom.Document()
#doc.appendChild(doc.createComment("Sample XML Document by richard"))

mdb = doc.createElementNS("https://schemas.isotc211.org/19115/-1/mdb/1.3.0", "mdb:MD_Metadata")
mdb.setAttribute("xmlns:mdb", "https://schemas.isotc211.org/19115/-1/mdb/1.3.0")
mdb.setAttribute("xmlns:gco", "https://schemas.isotc211.org/19103/-/gco/1.2.0")
mdb.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink")
mdb.setAttribute("xmlns:gml", "http://www.opengis.net/gml")
mdb.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance") 
mdb.setAttribute("xsi:schemaLocation", "http://www.isotc211.org/2000/mdb http://www.isotc211.org/2005/mdb/metadataEntity.xsd") 
doc.appendChild(mdb)

hierarchy1_parent1 = doc.createElement('mdb:fileIdentifier')
mdb.appendChild(hierarchy1_parent1)
filename = doc.createTextNode(fileName)
hierarchy1_parent1.appendChild(filename)

hierarchy2_parent1 = doc.createElement('mdb:characterSet')
mdb.appendChild(hierarchy2_parent1)

hierarchy2_child1 = doc.createElement('mdb:MD_CharacterSetCode')
hierarchy2_parent1.appendChild(hierarchy2_child1)
hierarchy2_child1.setAttribute('codeList', 'http://www.isotc211.org/2000/resources/codeList.xml#MD_CharacterSetCode')
hierarchy2_child1.setAttribute('codeListValue', 'utf-8') 
hierarchy2_child1.setAttribute('codeSpace', 'ISOTC211/19115')
utf8 = doc.createTextNode('utf-8')
hierarchy2_child1.appendChild(utf8)

hierarchy3_parent1 = doc.createElement('mdb:dateStamp')
mdb.appendChild(hierarchy3_parent1)
hierarchy3_child1 = doc.createElement('gco:DateTime')
hierarchy3_parent1.appendChild(hierarchy3_child1)
hierarchy3_child2 = doc.createElement('DateTimeOriginal')
hierarchy3_parent1.appendChild(hierarchy3_child2)
hierarchy3_child3 = doc.createElement('DateTimeDigitized')
hierarchy3_parent1.appendChild(hierarchy3_child3)

hierarchy4_parent1 = doc.createElement('mdb:spatialRepresentationInfo')
mdb.appendChild(hierarchy4_parent1)
hierarchy4_parent1.setAttribute('xlink:type', 'simple') 
hierarchy4_parent2 = doc.createElement('gml:coordinates')
hierarchy4_parent1.appendChild(hierarchy4_parent2)
hierarchy4_child1 = doc.createElement('gml:Point')
hierarchy4_parent2.appendChild(hierarchy4_child1)

hierarchy5_parent1 = doc.createElement('mdb:identificationInfo')
mdb.appendChild(hierarchy5_parent1)
hierarchy5_parent1.setAttribute('xlink:type', 'simple') 
hierarchy5_parent2 = doc.createElement('mdb:MD_DataIdentification')
hierarchy5_parent1.appendChild(hierarchy5_parent2)
hierarchy5_child1 = doc.createElement('gml:metaDataProperty')
hierarchy5_parent2.appendChild(hierarchy5_child1)
hierarchy5_child2 = doc.createElement('mdb:metadataExtensionInfo')
hierarchy5_child1.appendChild(hierarchy5_child2)

hierarchy5_child3 = doc.createElement('mdb:spatialResolution')
hierarchy5_parent2.appendChild(hierarchy5_child3)
hierarchy5_child4 = doc.createElement('mdb:MD_Resolution')
hierarchy5_child3.appendChild(hierarchy5_child4)
hierarchy5_child5 = doc.createElement('XResolution')
hierarchy5_child4.appendChild(hierarchy5_child5)
hierarchy5_child6 = doc.createElement('YResolution')
hierarchy5_child4.appendChild(hierarchy5_child6)
#hierarchy5_child7 = doc.createElement('ResolutionUnit')
#hierarchy5_child4.appendChild(hierarchy5_child7)
hierarchy5_child8 = doc.createElement('ExifImageWidth')
hierarchy5_child4.appendChild(hierarchy5_child8)
hierarchy5_child9 = doc.createElement('ExifImageHeight')
hierarchy5_child4.appendChild(hierarchy5_child9)

# fillout value for key in dom
jsonDictFile = fileName.lower()+'.dict'
fr = open(dictFilePath+'/'+jsonDictFile, "r")
dictStr = fr.read()
fr.close()

theDict = json.loads(dictStr)
for k, v in theDict.items():
    print(k, v)
    tag = k
    value = str(v)
    if tag not in usefullItems:
        ExtensionInfo = doc.createElement(tag)
        hierarchy5_child2.appendChild(ExtensionInfo)
        ExtensionInfotext = doc.createTextNode(value)
        ExtensionInfo.appendChild(ExtensionInfotext)
        
    # otherwise
    if tag == 'GPSLongitude':
            #long_lat = str(long_lat) <~ created done
            gpsLongValue = doc.createTextNode(value)
            hierarchy4_child1.appendChild(gpsLongValue)
            
    if tag == 'GPSLatitude':
            #long_lat = str(long_lat) <~ created done
            gpsLatValue = doc.createTextNode(value)
            hierarchy4_child1.appendChild(gpsLatValue)
            
    if tag == 'DateTime':
        value = value.replace(" ", "T")
        datetime = doc.createTextNode(value)
        hierarchy3_child1.appendChild(datetime)
    '''
    if tag == 'DateTimeOriginal':
        value = value.replace(" ", "T")
        dateoriginal = doc.createTextNode(value)
        hierarchy3_child2.appendChild(dateoriginal)

    if tag == 'DateTimeDigitized':
        value = value.replace(" ", "T")
        datedigitize = doc.createTextNode(value)
        hierarchy3_child3.appendChild(datedigitize)
    '''
    if tag == 'XResolution':
        xres = doc.createTextNode(value)
        hierarchy5_child5.appendChild(xres)

    if tag == 'YResolution':
        yres = doc.createTextNode(value)
        hierarchy5_child6.appendChild(yres)

    #if tag == 'ResolutionUnit':
    #    ures = doc.createTextNode(value)
    #    hierarchy5_child7.appendChild(ures)

    if tag == 'ExifImageWidth':
        Iwidth = doc.createTextNode(value)
        hierarchy5_child8.appendChild(Iwidth)

    if tag == 'ImageLength':
        Ilength = doc.createTextNode(value)
        hierarchy5_child9.appendChild(Ilength)

# write back to xml file
xmlStr = doc.toprettyxml(indent = '   ')
print('')
print(xmlStr)
wf = open(dictFilePath+'/'+fileName+'.xml', "w")
wf.write(xmlStr)
wf.close()
print(dictFilePath+'/'+fileName+'.xml'+' is created')

