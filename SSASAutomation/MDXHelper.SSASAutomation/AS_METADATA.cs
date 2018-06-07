using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace MDXHelper.SSASAutomation
{
    public interface AS_METADATA
    {
        DataTable GET_SSAS_DSV_SET(DB_SQLHELPER_BASE iCon);
        DataTable GET_SSAS_DIMENSION_SET(DB_SQLHELPER_BASE iCon);
        DataTable GET_SSAS_ATTRIBUTES_SET(DB_SQLHELPER_BASE iCon, String dimension_id = null);
        DataTable GET_SSAS_ATTRIBUTE_RELATION_SHIPS_SET(DB_SQLHELPER_BASE iCon, String dimension_id = null);
        DataTable GET_SSAS_HIERARCHIES_SET(DB_SQLHELPER_BASE iCon, String dimension_id = null);
        DataTable GET_SSAS_CORE_MEASURES_SET(DB_SQLHELPER_BASE iCon, String measure_group_id = null);
        DataTable GET_SSAS_MEASURE_GROUPS_SET(DB_SQLHELPER_BASE iHelper, int isRolap);
        DataTable GET_SSAS_PARTITION_SET(DB_SQLHELPER_BASE iCon, String measure_group_id = null);
        DataTable GET_SSAS_AGGREGATION_DESIGN_SET(DB_SQLHELPER_BASE iCon, String measure_group_id = null);
        DataTable GET_SSAS_DIM_USAGE_SET(DB_SQLHELPER_BASE iCon, String measure_group_id = null);
        String GET_SSAS_PARTITION_FILTER(String filterColumnName, int partitinIndex, int partitionMonthVolumn);
    }
}
