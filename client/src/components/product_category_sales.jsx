import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

import axios from 'axios';

const ProductCategorySales = () => {
    const [data, setData] = useState([])

    useEffect(() => {
        fetchData();
    }, [])

    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:5000/api/get/product_category_sales_by_month');
            setData(response.data["data"]); // Set the fetched data to state
            console.log(response.data["data"]);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    return (


        <>
            <div class="card text-bg-light mb-3" style={{ maxWidth: "50rem" }}>
                <div className = 'card-header'>
                    <h2 class="card-title">Sales per Category</h2>
                    <p class="card-title">by Month</p>
                </div>
                <div class="card-body">
                    

                    <AreaChart
                        width={700}
                        height={200}
                        data={data}
                        margin={{
                            top: 10,
                            right: 30,
                            left: 0,
                            bottom: 0,
                        }}
                    >

                        <XAxis dataKey="year_month" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Area type="monotone" dataKey="bed_bath_table" stackId="1" stroke="#C1F335" fill="#C1F335" />
                        <Area type="monotone" dataKey="computer_accessories" stackId="2" stroke="#F748B7" fill="#F748B7" />
                        <Area type="monotone" dataKey="health_beauty" stackId="3" stroke="#62D926" fill="#62D926" />
                        <Area type="monotone" dataKey="sports_leisure" stackId="4" stroke="#3838AB" fill="#3838AB" />
                        <Area type="monotone" dataKey="watches_gifts" stackId="5" stroke="#EE402E" fill="#EE402E" />
                    
                    </AreaChart>
                </div>
            </div>


        </>

    );
}

export default ProductCategorySales;