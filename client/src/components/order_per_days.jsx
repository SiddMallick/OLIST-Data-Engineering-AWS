import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

import axios from 'axios';

const OrderPerDays = () => {
    const [data, setData] = useState([])

    useEffect(()=>{
        fetchData();
    }, [])

    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:5000/api/get/orders_per_day');
            setData(response.data["data"]); // Set the fetched data to state
            console.log(response.data["data"]);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    }; 

    return (


        <>  

            <div class="card text-bg-light mb-3" style={{maxWidth : "28rem"}}>
                <div className='card-header'>
                    <h2 class="card-title">Orders per Day</h2>
                    <p class="card-title">Number of Orders per Day</p>
                </div>
                <div class="card-body">
                    
                    <AreaChart
                        width={400}
                        height={200}
                        data={data}
                        margin={{
                            top: 10,
                            right: 30,
                            left: 0,
                            bottom: 0,
                        }}
                    >

                        <XAxis dataKey="day" />
                        <YAxis />
                        <Tooltip />
                        <Area type="monotone" dataKey="orders" stackId="1" stroke="#C1F335" fill="#C1F335" />
                    </AreaChart>
                </div>
            </div>
            
            

        </>
            
    );
}

export default OrderPerDays;