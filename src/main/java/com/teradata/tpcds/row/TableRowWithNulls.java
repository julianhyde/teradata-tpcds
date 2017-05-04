/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teradata.tpcds.row;

import com.teradata.tpcds.generator.GeneratorColumn;
import com.teradata.tpcds.type.Date;

import static com.teradata.tpcds.type.Date.fromJulianDays;

public abstract class TableRowWithNulls
        implements TableRow
{
    private long nullBitMap;
    private GeneratorColumn firstColumn;

    protected TableRowWithNulls(long nullBitMap, GeneratorColumn firstColumn)
    {
        this.nullBitMap = nullBitMap;
        this.firstColumn = firstColumn;
    }

    private boolean isNull(GeneratorColumn column)
    {
        long kBitMask = 1L << (column.getGlobalColumnNumber() - firstColumn.getGlobalColumnNumber());
        return (nullBitMap & kBitMask) != 0;
    }

    protected <T> T getValueOrNull(T value, GeneratorColumn column)
    {
        return isNull(column) ? null : value;
    }

    protected long getLongOrNullForKey(long value, GeneratorColumn column)
    {
        return (isNull(column) || value == -1) ? null : value;
    }

    protected Date getDateOrNullFromJulianDays(long value, GeneratorColumn column)
    {
        return (isNull(column) || value < 0) ? null : fromJulianDays((int) value);
    }

    protected <T> String getStringOrNull(T value, GeneratorColumn column)
    {
        return isNull(column) ? null : value.toString();
    }

    protected String getStringOrNullForKey(long value, GeneratorColumn column)
    {
        return (isNull(column) || value == -1) ? null : Long.toString(value);
    }

    protected String getStringOrNullForBoolean(boolean value, GeneratorColumn column)
    {
        if (isNull(column)) {
            return null;
        }

        return value ? "Y" : "N";
    }

    protected String getDateStringOrNullFromJulianDays(long value, GeneratorColumn column)
    {
        return (isNull(column) || value < 0) ? null : fromJulianDays((int) value).toString();
    }
}
