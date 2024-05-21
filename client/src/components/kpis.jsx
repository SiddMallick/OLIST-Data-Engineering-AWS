import React, { useState, useEffect } from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts';

import axios from 'axios';

const KPIS = () => {
    const [data, setData] = useState([])

    useEffect(() => {
        fetchData();
    }, [])

    const fetchData = async () => {
        try {
            const response = await axios.get('http://127.0.0.1:5000/api/get/kpis');
            // const response = await axios.get('/api/get/kpis');

            setData(response.data); // Set the fetched data to state
            console.log(response.data);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    return (


        <>
            <div className='col'>
                <div class="card text-bg-success mb-3" style={{ maxWidth: "50rem" }}>
                    <div className='card-header'>
                        <h3 class="card-title">Products</h3>

                    </div>
                    <div class="card-body">
                        
                        <br></br>
                        <h1 class='card-text text-bold'>{data['number_of_products']}</h1>
                        <br></br>
                        
                    </div>
                </div>
            </div>
            <div className='col'>
                <div class="card text-bg-warning mb-3" style={{ maxWidth: "50rem" }}>
                    <div className='card-header'>
                        <h3 class="card-title">Customers</h3>

                    </div>
                    <div class="card-body">
                        
                        <br></br>
                        <h1 class='card-text text-bold'>{data['number_of_customers']}</h1>
                        <br></br>
                        
                    </div>
                </div>
            </div>
            <div className='col'>
                <div class="card text-bg-info mb-3" style={{ maxWidth: "50rem" }}>
                    <div className='card-header'>
                        <h3 class="card-title">Orders</h3>

                    </div>
                    <div class="card-body">
                        
                        <br></br>
                        <h1 class='card-text text-bold'>{data['number_of_orders']}</h1>
                        <br></br>
                        
                    </div>
                </div>
            </div>
            <div className='col'>
                <div class="card text-bg-danger mb-3" style={{ maxWidth: "50rem" }}>
                    <div className='card-header'>
                        <h3 class="card-title">Reviews</h3>

                    </div>
                    <div class="card-body">
                        
                        <br></br>
                        <h1 class='card-text text-bold'>{data['number_of_reviews']}</h1>
                        <br></br>
                        
                    </div>
                </div>
            </div>


        </>

    );
}

export default KPIS;