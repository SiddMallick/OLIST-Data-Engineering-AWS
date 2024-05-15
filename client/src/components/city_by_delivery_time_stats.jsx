import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, Rectangle, PieChart, Pie } from 'recharts';

import axios from 'axios';

const CityByDeliveryTimeStats = () => {
    const [data, setData] = useState([])

    useEffect(() => {
        fetchData();
    }, [])

    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:5000/api/get/city_by_delivery_time_stats');
            const jsonArray = response.data["data"]
            jsonArray.forEach(obj => {
                obj.total_sales = parseFloat(obj.total_sales);
            })

            jsonArray.sort((a, b) => b.total_sales - a.total_sales);
            setData(jsonArray); // Set the fetched data to state
            console.log(response.data["data"]);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    return (


        <>
            <div class="card text-bg-light mb-3" style={{ maxWidth: "100%" }}>
                <div className='card-header'>
                    <h2 class="card-title">Delivery Time Stats</h2>
                    <p class="card-title">based on Top 10 cities</p>
                </div>

                <div class="card-body">
                    <BarChart
                        width={1200}
                        height={300}
                        data={data}
                        margin={{
                            top: 5,
                            right: 30,
                            left: 40,
                            bottom: 5,
                        }}
                    >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="customer_city" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="carrier_days" stackId="a" fill="#F45D2C" activeBar={<Rectangle fill="#F8CAB9" stroke="#F8CAB9" />} />
                        <Bar dataKey="deliver_days" stackId="a" fill="#00A66E" activeBar={<Rectangle fill="#39F1A6" stroke="#39F1A6" />} />
                        <Bar dataKey="delay_days" stackId="a" fill="#FFA23A" activeBar={<Rectangle fill="#FCDCC1" stroke="#FCDCC1" />} />   
                    </BarChart>
                </div>
            </div>

        </>

    );
}

export default CityByDeliveryTimeStats;