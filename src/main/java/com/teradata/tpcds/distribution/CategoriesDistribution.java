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

package com.teradata.tpcds.distribution;

import com.google.common.collect.ImmutableList;
import com.teradata.tpcds.distribution.DistributionUtils.WeightsBuilder;
import com.teradata.tpcds.random.RandomNumberStream;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;
import static com.teradata.tpcds.distribution.DistributionUtils.getDistributionIterator;
import static com.teradata.tpcds.distribution.DistributionUtils.getListFromCommaSeparatedValues;
import static java.lang.Integer.parseInt;

public class CategoriesDistribution implements Distribution
{
    private static final int NUM_WEIGHT_FIELDS = 1;
    private static final String VALUES_AND_WEIGHTS_FILENAME = "categories.dst";
    static final CategoriesDistribution CATEGORIES_DISTRIBUTION = buildCategoriesDistribution();

    private final ImmutableList<String> names;
    private final ImmutableList<String> classes;
    private final ImmutableList<Integer> hasSizes;
    private final ImmutableList<Integer> weights;

    private CategoriesDistribution(ImmutableList<String> names, ImmutableList<String> classes, ImmutableList<Integer> hasSizes, ImmutableList<Integer> weights)
    {
        this.names = names;
        this.classes = classes;
        this.hasSizes = hasSizes;
        this.weights = weights;
    }

    private static CategoriesDistribution buildCategoriesDistribution()
    {
        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> classesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Integer> hasSizesBuilder = ImmutableList.builder();
        WeightsBuilder weightsBuilder = new WeightsBuilder();

        Iterator<List<String>> iterator = getDistributionIterator(VALUES_AND_WEIGHTS_FILENAME);
        while (iterator.hasNext()) {
            List<String> fields = iterator.next();
            checkState(fields.size() == 2, "Expected line to contain 2 parts but it contains %d: %s", fields.size(), fields);

            List<String> values = getListFromCommaSeparatedValues(fields.get(0));
            checkState(values.size() == 3, "Expected line to contain 3 values, but it contained %d, %s", values.size(), values);

            namesBuilder.add(values.get(0));
            classesBuilder.add(values.get(1));
            hasSizesBuilder.add(parseInt(values.get(2)));

            List<String> weights = getListFromCommaSeparatedValues(fields.get(1));
            checkState(weights.size() == NUM_WEIGHT_FIELDS, "Expected line to contain %d weights, but it contained %d, %s", NUM_WEIGHT_FIELDS, weights.size(), values);
            weightsBuilder.computeAndAddNextWeight(parseInt(weights.get(0)));
        }

        return new CategoriesDistribution(namesBuilder.build(),
                classesBuilder.build(),
                hasSizesBuilder.build(),
                weightsBuilder.build());
    }

    public static Integer pickRandomIndex(RandomNumberStream stream)
    {
        return DistributionUtils.pickRandomIndex(CATEGORIES_DISTRIBUTION.weights, stream);
    }

    public static String getCategoryAtIndex(int index)
    {
        return CATEGORIES_DISTRIBUTION.names.get(index);
    }

    public static int getHasSizeAtIndex(int index)
    {
        return CATEGORIES_DISTRIBUTION.hasSizes.get(index);
    }

    private List list(int field)
    {
        switch (field) {
        case 0:
            return names;
        case 1:
            return classes;
        case 2:
            return hasSizes;
        default:
            throw new IllegalArgumentException("unknown field " + field
                    + " in distribution " + this);
        }
    }

    public Object cell(int field, int row)
    {
        return list(field).get(row);
    }

    public Object random(int field, int weight, Random random)
    {
        List list = list(field);
        return list.get(random.nextInt(list.size()));
    }

    public List<String> getWeightNames()
    {
        return ImmutableList.of("uniform");
    }

    public int getSize()
    {
        return list(0).size();
    }
}
