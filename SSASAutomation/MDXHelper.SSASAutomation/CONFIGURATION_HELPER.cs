using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;

namespace MDXHelper.SSASAutomation
{
    public static class CONFIGURATION_HELPER
    {
        #region 获取属性的值
        /// <summary>
        /// 获取属性的值
        /// </summary>
        /// <param name="_propertyName"></param>
        /// <returns></returns>
        public static String GET_METADATA_PROPERTY(String _propertyName)
        {
            String property_value = "";
            String cubeProcessXMLAPath = CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER + @"\SSASConfiguration\Setting.xml";
            XmlDocument xml_doc = new XmlDocument();
            xml_doc.Load(cubeProcessXMLAPath);
            XmlNode root = xml_doc.SelectSingleNode("configurations");
            try
            {
                foreach (XmlNode property in root.ChildNodes)
                {
                    String property_name = property.Attributes["name"].Value;
                    if (property_name == _propertyName)
                    {
                        foreach (XmlNode sub_property in property.ChildNodes)
                        {
                            if (sub_property.Name.ToLower() == "value")
                            {
                                property_value = sub_property.InnerText;
                                break;
                            }
                        }
                        break;
                    }
                }

            }
            catch (Exception ex)
            {
                //ADD_MESSAGE_LOG(ex.Message, MESSAGE_TYPE.READ_SYSTEM_FILE, MESSAGE_RESULT_TYPE.Error);
                throw;
            }
            return property_value;
        }
        #endregion

        public static String BASIC_CONFIGURATION_FOLDER;
    }
}