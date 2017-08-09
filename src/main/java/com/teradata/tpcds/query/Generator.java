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

package com.teradata.tpcds.query;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/** Context for converting a {@link Query} into text, expanding all of its
 * embedded substitutions.
 */
class Generator
{
    // From TPC-DS spec, table 3-2 "Database Row Counts", for 1G sizing.
    private static final ImmutableMap<String, Integer> TABLE_ROW_COUNTS =
        ImmutableMap.<String, Integer>builder()
            .put("CALL_CENTER", 8)
            .put("CATALOG_PAGE", 11718)
            .put("CATALOG_RETURNS", 144067)
            .put("CATALOG_SALES", 1441548)
            .put("CUSTOMER", 100000)
            .put("CUSTOMER_ADDRESS", 50000)
            .put("CUSTOMER_DEMOGRAPHICS", 1920800)
            .put("DATE_DIM", 73049)
            .put("DBGEN_VERSION", 1)
            .put("HOUSEHOLD_DEMOGRAPHICS", 7200)
            .put("INCOME_BAND", 20)
            .put("INVENTORY", 11745000)
            .put("ITEM", 18000)
            .put("PROMOTION", 300)
            .put("REASON", 35)
            .put("SHIP_MODE", 20)
            .put("STORE", 12)
            .put("STORE_RETURNS", 287514)
            .put("STORE_SALES", 2880404)
            .put("TIME_DIM", 86400)
            .put("WAREHOUSE", 5)
            .put("WEB_PAGE", 60)
            .put("WEB_RETURNS", 71763)
            .put("WEB_SALES", 719384)
            .put("WEB_SITE", 1)
            .build();

    final Random random;
    final ImmutableMap<String, Substitution> substitutions;
    final Map<String, Object> substitutionValues = new HashMap<>();

    /**
     * Creates a generator.
     *
     * @param random Random-number generator
     * @param substitutions Substitutions, by name
     */
    Generator(Random random, ImmutableMap<String, Substitution> substitutions)
    {
        this.random = random;
        this.substitutions = substitutions;
    }

    public String rowCount(String relation)
    {
        return TABLE_ROW_COUNTS.get(relation.toUpperCase(Locale.ROOT)) + "";
    }
}
