using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDXHelper.SSASAutomation
{
    public enum MESSAGE_TYPE
    {
        DSV,
        DIMENSION,
        ADD_DIMENSION,
        ADD_CUBE_DIMENSION,
        ATTRIBUTE,
        ATTRIBUTE_RELATIONSHIP,
        HIERARCHIES,
        MEASURE_GROUP,
        MEASURES,
        PARTITION,
        AGGREGATION_DESIGN,
        DIM_USAGE,
        SQLQuery,
        CREATE_CUBE,
        COLUMN_BINDING_DATA_ITEM,
        DIM_USAGE_REGULAR,
        DIM_USAGE_REFERENCE,
        DIM_USAGE_FACT,
        DIM_USAGE_MANY_TO_MANY,
        CUBE_PROCESS,
        MDX,
        READ_SYSTEM_FILE
    }
}
