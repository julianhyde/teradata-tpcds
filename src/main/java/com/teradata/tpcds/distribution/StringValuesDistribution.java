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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.teradata.tpcds.distribution.DistributionUtils.getDistributionIterator;
import static com.teradata.tpcds.distribution.DistributionUtils.getListFromCommaSeparatedValues;

public class StringValuesDistribution extends DistributionBase<String>
{
    public StringValuesDistribution(ImmutableList<ImmutableList<String>> valuesLists, ImmutableList<ImmutableList<Integer>> weightsLists)
    {
        super(valuesLists, weightsLists, generateWeightNames(weightsLists.size()));
    }

    public StringValuesDistribution(ImmutableList<ImmutableList<String>> valuesLists,
            ImmutableList<ImmutableList<Integer>> weightsLists,
            List<String> weightNames)
    {
        super(valuesLists, weightsLists, weightNames);
    }

    public static StringValuesDistribution buildStringValuesDistribution(String valuesAndWeightsFilename, int numValueFields, int numWeightFields)
    {
        return buildStringValuesDistribution(valuesAndWeightsFilename,
                numValueFields, DistributionBase.generateWeightNames(numWeightFields));
    }

    public static StringValuesDistribution buildStringValuesDistribution(String valuesAndWeightsFilename, int numValueFields, List<String> weightFieldNames)
    {
        Iterator<List<String>> iterator = getDistributionIterator(valuesAndWeightsFilename);

        List<ImmutableList.Builder<String>> valuesBuilders = new ArrayList<>(numValueFields);
        for (int i = 0; i < numValueFields; i++) {
            valuesBuilders.add(ImmutableList.<String>builder());
        }

        final int numWeightFields = weightFieldNames.size();
        List<WeightsBuilder> weightsBuilders = new ArrayList<>(numWeightFields);
        for (int i = 0; i < numWeightFields; i++) {
            weightsBuilders.add(new WeightsBuilder());
        }

        while (iterator.hasNext()) {
            List<String> fields = iterator.next();
            checkState(fields.size() == 2, "Expected line to contain 2 parts but it contains %s: %s", fields.size(), fields);

            List<String> values = getListFromCommaSeparatedValues(fields.get(0));
            checkState(values.size() == numValueFields, "Expected line to contain %s values, but it contained %s, %s", numValueFields, values.size(), values);
            for (int i = 0; i < values.size(); i++) {
                valuesBuilders.get(i).add(values.get(i));
            }

            List<String> weights = getListFromCommaSeparatedValues(fields.get(1));
            checkState(weights.size() == numWeightFields, "Expected line to contain %s weights, but it contained %s, %s", numWeightFields, weights.size(), weights);
            for (int i = 0; i < weights.size(); i++) {
                weightsBuilders.get(i).computeAndAddNextWeight(Integer.parseInt(weights.get(i)));
            }
        }

        ImmutableList.Builder<ImmutableList<String>> valuesListsBuilder = ImmutableList.<ImmutableList<String>>builder();
        for (ImmutableList.Builder<String> valuesBuilder : valuesBuilders) {
            valuesListsBuilder.add(valuesBuilder.build());
        }
        ImmutableList<ImmutableList<String>> valuesLists = valuesListsBuilder.build();

        ImmutableList.Builder<ImmutableList<Integer>> weightsListBuilder = ImmutableList.<ImmutableList<Integer>>builder();
        for (WeightsBuilder weightsBuilder : weightsBuilders) {
            weightsListBuilder.add(weightsBuilder.build());
        }
        ImmutableList<ImmutableList<Integer>> weightsLists = weightsListBuilder.build();
        return new StringValuesDistribution(valuesLists, weightsLists, weightFieldNames);
    }

    public String pickRandomValue(int valueListIndex, int weightListIndex, RandomNumberStream stream)
    {
        checkArgument(valueListIndex < valuesLists.size(), "index out of range, max value index is " + (valuesLists.size() - 1));
        checkArgument(weightListIndex < weightsLists.size(), "index out of range, max weight index is " + (weightsLists.size() - 1));
        return DistributionUtils.pickRandomValue(valuesLists.get(valueListIndex), weightsLists.get(weightListIndex), stream);
    }

    public String getValueForIndexModSize(long index, int valueListIndex)
    {
        checkArgument(valueListIndex < valuesLists.size(), "index out of range, max value index is " + (valuesLists.size() - 1));
        return DistributionUtils.getValueForIndexModSize(index, valuesLists.get(valueListIndex));
    }

    public int pickRandomIndex(int weightListIndex, RandomNumberStream stream)
    {
        checkArgument(weightListIndex < weightsLists.size(), "index out of range, max weight index is " + (weightsLists.size() - 1));
        return DistributionUtils.pickRandomIndex(weightsLists.get(weightListIndex), stream);
    }

    public int getWeightForIndex(int index, int weightListIndex)
    {
        checkArgument(weightListIndex < weightsLists.size(), "index out of range, max weight index is " + (weightsLists.size() - 1));
        return DistributionUtils.getWeightForIndex(index, weightsLists.get(weightListIndex));
    }

    public String getValueAtIndex(int valueListIndex, int valueIndex)
    {
        return list(valueListIndex).get(valueIndex);
    }
}
