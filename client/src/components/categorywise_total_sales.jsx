import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, Rectangle, PieChart, Pie } from 'recharts';

import axios from 'axios';

const CustomerwiseTotalSales = () => {
    const [data, setData] = useState([])

    useEffect(() => {
        fetchData();
    }, [])

    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:5000/api/get/categorywise_total_sales');
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
            <div class="card text-bg-light mb-3" style={{maxWidth : "100%"}}>
                <div className='card-header'>
                    <h2 class="card-title">Category-wise total sales</h2>
                    <p class="card-title">Total Sales per Category</p>
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
                        <XAxis dataKey="category" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="total_sales" fill="#D24317" activeBar={<Rectangle fill="#FEB670" stroke="#FEB670" />} />

                    </BarChart>
                </div>
            </div> 
            
        </>

    );
}

export default CustomerwiseTotalSales;