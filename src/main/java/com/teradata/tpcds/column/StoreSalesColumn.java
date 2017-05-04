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
package com.teradata.tpcds.column;

import com.teradata.tpcds.Table;
import com.teradata.tpcds.row.StoreSalesRow;
import com.teradata.tpcds.type.Decimal;

import static com.teradata.tpcds.Table.STORE_SALES;
import static com.teradata.tpcds.column.ColumnTypes.IDENTIFIER;
import static com.teradata.tpcds.column.ColumnTypes.INTEGER;
import static com.teradata.tpcds.column.ColumnTypes.decimal;

public enum StoreSalesColumn
        implements Column
{
    SS_SOLD_DATE_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsSoldDateSk();
                }
            },
    SS_SOLD_TIME_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsSoldTimeSk();
                }
            },
    SS_ITEM_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsItemSk();
                }
            },
    SS_CUSTOMER_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsCustomerSk();
                }
            },
    SS_CDEMO_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsCdemoSk();
                }
            },
    SS_HDEMO_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsHdemoSk();
                }
            },
    SS_ADDR_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsAddrSk();
                }
            },
    SS_STORE_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsStoreSk();
                }
            },
    SS_PROMO_SK(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsPromoSk();
                }
            },
    SS_TICKET_NUMBER(IDENTIFIER)
            {
                public long getIdentifier(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsTicketNumber();
                }
            },
    SS_QUANTITY(INTEGER)
            {
                public long getInteger(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsQuantity();
                }
            },
    SS_WHOLESALE_COST(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsWholesaleCost();
                }
            },
    SS_LIST_PRICE(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsListPrice();
                }
            },
    SS_SALES_PRICE(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsSalesPrice();
                }
            },
    SS_EXT_DISCOUNT_AMT(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsExtDiscountAmount();
                }
            },
    SS_EXT_SALES_PRICE(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsExtSalesPrice();
                }
            },
    SS_EXT_WHOLESALE_COST(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsExtWholesaleCost();
                }
            },
    SS_EXT_LIST_PRICE(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsExtListPrice();
                }
            },
    SS_EXT_TAX(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsExtTax();
                }
            },
    SS_COUPON_AMT(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsCouponAmount();
                }
            },
    SS_NET_PAID(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsNetPaid();
                }
            },
    SS_NET_PAID_INC_TAX(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsNetPaidIncludingTax();
                }
            },
    SS_NET_PROFIT(decimal(7, 2))
            {
                public Decimal getDecimal(StoreSalesRow storeSalesRow)
                {
                    return storeSalesRow.getSsNetProfit();
                }
            };

    private final ColumnType type;

    StoreSalesColumn(ColumnType type)
    {
        this.type = type;
    }

    @Override
    public Table getTable()
    {
        return STORE_SALES;
    }

    @Override
    public String getName()
    {
        return name().toLowerCase();
    }

    @Override
    public ColumnType getType()
    {
        return type;
    }

    @Override
    public int getPosition()
    {
        return ordinal();
    }
}
