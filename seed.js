// backend/seed.js
require('dotenv').config(); 
const mysql = require('mysql2/promise');
const bcrypt = require('bcryptjs');

async function seedHeadNurse() {
    let connection;
    try {
        connection = await mysql.createConnection({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_DATABASE,
            port: process.env.DB_PORT || 4000,
            ssl: { 
                minVersion: 'TLSv1.2', 
                rejectUnauthorized: false // เปลี่ยนเป็น false หากเจอปัญหา Self-signed certificate
            }
        });
        console.log('🔌 Connected to Database');

        const plainPassword = 'admin1234';
        const hashedPassword = await bcrypt.hash(plainPassword, 10);

        const headNurse = {
            FirstName: 'ลมัย',  
            LastName: 'บุยธรรม',    
            Email: 'dungkamoljoajit2547@gmail.com', 
            PasswordHash: hashedPassword, 
            RoleID: 1,
            MustChangePassword: 1
        };

        // ตรวจสอบ Email ซ้ำ
        const [rows] = await connection.execute(
            'SELECT Email FROM User WHERE Email = ?', 
            [headNurse.Email]
        );

        if (rows.length > 0) {
            console.log(`⚠️ Head Nurse (${headNurse.Email}) already exists. Skipping...`);
        } else {
            const sql = `
                INSERT INTO User (FirstName, LastName, Email, PasswordHash, RoleID, MustChangePassword)
                VALUES (?, ?, ?, ?, ?, ?)
            `;
            
            await connection.execute(sql, [
                headNurse.FirstName,
                headNurse.LastName,
                headNurse.Email,
                headNurse.PasswordHash,
                headNurse.RoleID,
                headNurse.MustChangePassword
            ]);
            
            console.log('✅ Created Head Nurse successfully!');
            console.log(`👉 Email: ${headNurse.Email}`);
            console.log(`👉 Password: ${plainPassword}`);
        }

    } catch (error) {
        // ✅ ตรงนี้สำคัญมาก: มันจะบอกสาเหตุจริงๆ
        console.error('❌ เกิดข้อผิดพลาดตอน Seed:');
        console.error('--- ข้อมูล Error ---');
        console.error('Code:', err.code);     // เช่น ECONNREFUSED (ลืมเปิด DB)
        console.error('Message:', err.message); // เช่น Access denied (รหัสผิด)
        console.error('-------------------');
    } finally {
        if (connection) {
            await connection.end();
            console.log('🚪 Connection closed.');
        }
    }
}

seedHeadNurse();