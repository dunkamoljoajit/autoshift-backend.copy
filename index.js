process.env.TZ = 'Asia/Bangkok'; // ตั้งค่า Timezone ของ Node.js ให้เป็นไทยทันทีที่เริ่มทำงาน
const fs = require('fs');
const express = require('express');
const app = express();
const mysql = require('mysql2');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const nodemailer = require('nodemailer'); // ✅ แก้ไข: เปิดใช้งาน Nodemailer
const multer = require('multer');
const path = require('path');
const jwt = require('jsonwebtoken');
const xlsx = require('xlsx');
const port = process.env.PORT || 3000;
const cloudinary = require('cloudinary').v2;
const moment = require('moment');
const crypto = require('crypto');
const Joi = require('joi');
const { CloudinaryStorage } = require('multer-storage-cloudinary');
const cron = require('node-cron');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const mysqldump = require('mysqldump');
const { exec } = require('child_process');
const PQueue = require('p-queue').default;

// ✅ [Socket.io Step 1] Import HTTP และ Socket.io
const http = require('http');
const { Server } = require('socket.io');
const axios = require("axios");

require('dotenv').config();
const JWT_SECRET = process.env.JWT_SECRET;

// ==========================================
// --- SERVER & SOCKET SETUP ---
// ==========================================
const allowedOrigins = [
    {ลิงก์ Frontend}',
    'http://localhost:5500', // สำหรับTest ในเครื่อง
    'http://127.0.0.1:5500'
];

// ✅ สร้าง HTTP Server ครอบ Express App
const server = http.createServer(app);

// ✅ ตั้งค่า Socket.io
const io = new Server(server, {
    cors: {
        origin: allowedOrigins,
        methods: ["GET", "POST"]
    }
});

const otpLimiter = rateLimit({
    windowMs: 60 * 60 * 1000, // 1 ชั่วโมง
    max: 3, // ขอได้ไม่เกิน 3 ครั้งต่อชั่วโมงต่อหนึ่ง IP
    message: { success: false, message: "⛔ คุณขอ OTP บ่อยเกินไป กรุณารออีก 1 ชั่วโมง" },
    standardHeaders: true,
    legacyHeaders: false,
});
// --- Security Middleware ---
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    
    if (!token) return res.status(401).json({ success: false, message: 'Access Denied: No Token Provided' });

    jwt.verify(token, JWT_SECRET, async (err, decoded) => {
        if (err) {
            return res.status(401).json({ success: false, message: 'Invalid or Expired Token' });
        }

        try {
            // [จุดที่เพิ่ม]: ตรวจสอบเลขเวอร์ชันจาก Database แบบ Real-time
            const [rows] = await dbPool.query("SELECT token_version FROM User WHERE UserID = ?", [decoded.userId]);
            const user = rows[0];

            // ถ้าเลขในกุญแจ (decoded.token_version) ไม่ตรงกับในสมุดจด (user.token_version)
            if (!user || user.token_version !== decoded.token_version) {
                return res.status(401).json({ 
                    success: false, 
                    forceLogout: true, // แจ้ง Frontend ให้ล้าง localStorage และเด้งไปหน้า Login
                    message: 'รหัสผ่านของคุณถูกเปลี่ยนแล้ว กรุณาเข้าสู่ระบบใหม่' 
                });
            }

            req.user = decoded;
            next();
        } catch (dbErr) {
            return res.status(500).json({ success: false, message: 'Database Error' });
        }
    });
};

// ✅ 1. เปลี่ยนการประกาศตัวแปรเก็บ User ที่ออนไลน์
let onlineUsers = new Map(); 

io.on('connection', (socket) => {
    console.log('⚡ Client connected:', socket.id);

    // ✅ 2. แก้ไขตอน Register: เก็บเข้า Set เพื่อรองรับหลายหน้าจอ (Multi-tab)
    socket.on('register_user', (userId) => {
        if (userId) {
            const uid = String(userId);
            if (!onlineUsers.has(uid)) {
                onlineUsers.set(uid, new Set()); // ถ้ายังไม่มี User นี้ ให้สร้าง Set ใหม่
            }
            onlineUsers.get(uid).add(socket.id); // เพิ่ม SocketID เข้าไปในกลุ่มของ User นั้น
            console.log(`✅ User ${uid} Online (Total Tabs: ${onlineUsers.get(uid).size})`);
        }
    });

    // ✅ 3. แก้ไขตอน Disconnect: ลบเฉพาะ SocketID ที่ปิดไป
    socket.on('disconnect', () => {
        for (let [uid, sessions] of onlineUsers.entries()) {
            if (sessions.has(socket.id)) {
                sessions.delete(socket.id); // ลบ Socket ที่ปิดออก
                if (sessions.size === 0) {
                    onlineUsers.delete(uid); // ถ้าไม่เหลือหน้าจอที่เปิดอยู่เลย ให้ลบ User ออกจากรายการ Online
                }
                console.log(`❌ A tab of User ${uid} closed`);
                break;
            }
        }
    });
});

// ✅ ฟังก์ชันช่วยส่ง Notification แบบ Real-time
async function sendRealTimeNotification(targetUserId, data) {
    try {
        const sessions = onlineUsers.get(String(targetUserId));
        if (sessions) {
            // ดึง Unread Count ล่าสุดจาก DB
            const [systemNotis] = await dbPool.query("SELECT COUNT(*) as count FROM Notifications WHERE UserID = ? AND IsRead = 0", [targetUserId]);
            
            // ส่งข้อมูลไปทุก Tab ที่ User คนนี้เปิดอยู่
            sessions.forEach(socketId => {
                io.to(socketId).emit('receive_notification', {
                    ...data,
                    unreadCount: systemNotis[0].count // พ่วงเลขแจ้งเตือนไปด้วย
                });
            });
            console.log(`🚀 Sent Real-time to User ${targetUserId}`);
        }
    } catch (err) {
        console.error("Socket Notification Error:", err);
    }
}

async function sendEmailNotification(targetUserId, subject, htmlContent) {
    try {
        // 1. ดึง Email และชื่อจาก DB
        const [users] = await dbPool.query("SELECT Email, FirstName FROM User WHERE UserID = ?", [targetUserId]);
        if (users.length === 0) return;

        const { Email, FirstName } = users[0];

        // 2. ตั้งค่ารูปแบบเมล
        const mailOptions = {
            from: '"AUTONURSESHIFT System" <autonurseshift@gmail.com>', //แก้ให้เปลี่ยน autonurseshift@gmail.com เป็นอีเมลที่น้องใช้สมัครและ Verify ใน Brevo (ไม่งั้นเมลจะส่งไม่ออกเพราะ Brevo ไม่อนุญาตให้ใช้อีเมลคนอื่นส่ง)
            to: Email,
            subject: subject,
            html: `
                <div style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #e0e0e0; border-radius: 12px; padding: 25px;">
                    <h2 style="color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 10px;">สวัสดีคุณ ${FirstName}</h2>
                    <div style="font-size: 16px; color: #333; line-height: 1.6; margin-top: 20px;">
                        ${htmlContent}
                    </div>
                    <div style="margin-top: 30px; text-align: center;">
                        <a href="https://autoshift-frontend.vercel.app" style="background-color: #007bff; color: white; padding: 12px 25px; text-decoration: none; border-radius: 8px; font-weight: bold;">เข้าสู่ระบบ AUTONURSESHIFT</a>
                    </div>  //ให้น้องเปลี่ยน https://autoshift-frontend.vercel.app เป็น URL เว็บไซต์ที่น้อง Deploy Frontend ของตัวเอง
                    <p style="margin-top: 40px; color: #999; font-size: 12px; text-align: center; border-top: 1px solid #eee; padding-top: 10px;">
                        นี่คือการแจ้งเตือนอัตโนมัติ กรุณาอย่าตอบกลับอีเมลฉบับนี้
                    </p>
                </div>
            `
        };

        await transporter.sendMail(mailOptions);
        console.log(`📧 Email sent to: ${Email}`);
    } catch (err) {
        console.error("❌ Email Notification Error:", err.message);
    }
}

// ==========================================
// --- SECURITY CONFIGURATION ---
// ==========================================

const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, 
    max: 10, 
    message: { message: "⛔ คุณพยายามเข้าระบบมากเกินไป กรุณารอ 15 นาที" },
    standardHeaders: true,
    legacyHeaders: false,
});

app.use(helmet()); 
app.get('/api/cron-ping', (req, res) => {
    console.log("Cron Ping Received!");
    res.status(200).send("OK");
});
app.use(cors({
    origin: function (origin, callback) {
        // อนุญาตถ้าไม่มี origin (เช่น mobile app) หรืออยู่ใน whitelist
        if (!origin || allowedOrigins.indexOf(origin) !== -1) {
            callback(null, true);
        } else {
            callback(new Error('Not allowed by CORS'));
        }
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true,
    allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());
app.set('trust proxy', 1); 

// ==========================================
// 1. CONFIGURATION: Cloudinary
// ==========================================
cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
});

const storage = new CloudinaryStorage({
    cloudinary: cloudinary,
    params: {
        folder: 'autonurseshift-profiles',
        allowed_formats: ['jpg', 'png', 'jpeg'],
        transformation: [
            { width: 400, height: 400, crop: 'fill', gravity: 'face' },
            { quality: 'auto', fetch_format: 'auto' }
        ]
    },
});
const upload = multer({ storage: storage });
function getPublicIdFromUrl(url) {
    const parts = url.split('/');
    const fileName = parts[parts.length - 1];
    const publicId = fileName.split('.')[0];
    return `autonurseshift-profiles/${publicId}`;
}

// ==========================================
// 2. CONFIGURATION: Excel Upload
// ==========================================
const excelStorage = multer.memoryStorage();
const excelFilter = (req, file, cb) => {
    if (file.mimetype.includes('excel') || file.mimetype.includes('spreadsheetml')) {
        cb(null, true);
    } else {
        cb(new Error('กรุณาอัปโหลดเฉพาะไฟล์ Excel (.xlsx)'), false);
    }
};
const uploadExcel = multer({ storage: excelStorage, fileFilter: excelFilter , limits: { fileSize: 5 * 1024 * 1024 } });

// ==========================================
// 3. DATABASE CONNECTION
// ==========================================
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT || 4000,

  waitForConnections: true,
  connectionLimit: 8,   // ⭐ ลดสำหรับ TiDB Serverless
  queueLimit: 0,

  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
  timezone: '+07:00',

  ssl: {
    minVersion: 'TLSv1.2',
    rejectUnauthorized: true
  }
};
const dbPool = mysql.createPool(dbConfig).promise();

const dbQueue = new PQueue({ concurrency: 4 });
// =======================
// SAFE QUERY (กัน TiDB ไม่มี instance)
// =======================
async function query(sql, params = [], retry = 3) {
  return dbQueue.add(async () => {
    try {
      return await dbPool.query(sql, params);
    } catch (err) {
      if (
        err.code === 'ER_UNKNOWN_ERROR' &&
        err.sqlMessage?.includes('No available TiDB instances') &&
        retry > 0
      ) {
        console.log("⏳ TiDB busy → retry", retry);
        await new Promise(r => setTimeout(r, 1500));
        return query(sql, params, retry - 1);
      }
      throw err;
    }
  });
}

module.exports = { query, dbPool };

// ✅ BREVO CONFIGURATION (ใช้ Port 2525 ทะลุ Block)
const transporter = nodemailer.createTransport({
    host: 'smtp-relay.brevo.com', 
    port: 2525, // ⚠️ เปลี่ยนเป็น 2525 (Port นี้ Render ไม่บล็อก)
    secure: false, // Port 2525 ไม่ใช้ SSL
    auth: {
        user: process.env.EMAIL_USER, 
        pass: process.env.EMAIL_PASS  
    },
    tls: {
        rejectUnauthorized: false
    },
    connectionTimeout: 10000,
    greetingTimeout: 5000
});

// เช็กการเชื่อมต่อ
transporter.verify((error, success) => {
    if (error) {
        console.log("❌ Brevo Error:", error);
    } else {
        console.log("✅ Brevo Connected on Port 2525! Ready to send.");
    }
});

// ==========================================
// 4. HELPER FUNCTIONS
// ==========================================

// ✅ ฟังก์ชันดึงเวลาไทยที่แม่นยำที่สุด (ใช้ Moment.js)
// ใช้สำหรับ Insert ลง DB โดยตรง เพื่อความชัวร์ 100%
function getThaiTimeInMySQLFormat(addMinutes = 0) {
    return moment().utcOffset(7).add(addMinutes, 'minutes').format('YYYY-MM-DD HH:mm:ss');
}

// ✅ ฟังก์ชัน Helper สำหรับวันที่ปัจจุบันแบบ YYYY-MM-DD (ใช้ใน Logic จัดเวร)
function getTodayDateString() {
    return moment().utcOffset(7).format('YYYY-MM-DD');
}

// ❌ ลบฟังก์ชันซ้ำซ้อนออกแล้ว

async function logLoginAttempt(dbPool, data) {
    try {
        const { UserID, Email, IP, Status, FailureReason } = data;
        // ✅ แก้ไข: ใช้เวลาจาก JS แทน DATE_ADD SQL
        const sql = `INSERT INTO LoginLog (UserID, AttemptedEmail, Status, IP_Address, FailureReason, CreatedAt) VALUES (?, ?, ?, ?, ?, ?)`;
        await dbPool.query(sql, [UserID || null, Email, Status, IP, FailureReason || null, getThaiTimeInMySQLFormat()]);
    } catch (err) { console.error("Log Error:", err.message); }
}

function generateOTP() {
    return crypto.randomInt(100000, 1000000).toString();
}

function generateRandomPassword(length = 8) {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let retVal = "";
    try {
        for (let i = 0; i < length; ++i) {
            const randomIndex = crypto.randomInt(0, charset.length);
            retVal += charset.charAt(randomIndex);
        }
    } catch (err) {
        console.error("Crypto Error:", err);
        return crypto.randomBytes(length).toString('hex').slice(0, length);
    }
    return retVal;
}

async function checkFatigueStatus(dbPool, userId, targetDate, targetShiftId, options = {}) {
    try {
        const { ignoreWeeklyLimit = false, excludeScheduleId = null } = options;
        
        // 1. ดึงเวรในวันที่เป้าหมาย (SELECT มาให้ครบทุกคอลัมน์ที่ต้องใช้)
        const [existing] = await dbPool.query(
            `SELECT NS.*, S.ShiftName, S.StartTime, S.EndTime 
             FROM NurseSchedule NS 
             JOIN Shift S ON NS.Shift_id = S.Shift_id 
             WHERE NS.UserID = ? AND NS.Nurse_Date = ?`, 
            [userId, targetDate]
        );

        for (const row of existing) {
            // ดึง ID ออกมาแบบปลอดภัย (รองรับทั้ง Shift_id, shift_id, ShiftID)
            const rowShiftId = row.Shift_id || row.shift_id || row.ShiftID;
            const rowScheduleId = row.ScheduleID || row.schedule_id || row.Scheduleid;

            if (excludeScheduleId && Number(rowScheduleId) === Number(excludeScheduleId)) continue;
            
            // กฎข้อ 1: ห้ามเวรซ้อน
            if (Number(rowShiftId) === Number(targetShiftId)) {
                return { safe: false, message: `คุณมีเวร ${row.ShiftName || 'นี้'} ในวันนี้อยู่แล้ว` };
            }
            
            // กฎข้อ 2: ห้ามควบ บ่าย (2) + ดึก (3) ในวันเดียวกัน
            if ((Number(targetShiftId) === 2 && Number(rowShiftId) === 3) || 
                (Number(targetShiftId) === 3 && Number(rowShiftId) === 2)) {
                return { safe: false, message: "ผิดกฎวอร์ด: ห้ามควงเวร บ่าย-ต่อ-ดึก ในวันเดียวกัน" };
            }
        }
        
        // กฎข้อ 3: จำกัดสูงสุด 2 เวร/วัน
        if (existing.length >= 2) return { safe: false, message: "เต็มโควตา 2 เวรต่อวันแล้ว" };

        // กฎข้อ 3.5: แจ้งเตือนการควงเวร (กรณีมี 1 เวรอยู่แล้ว)
        if (existing.length === 1 && !excludeScheduleId) {
            return { safe: true, isWarning: true, message: "⚠️ แจ้งเตือน: รายการนี้จะเป็นการควงเวร (2 เวรในวันเดียว)" };
        }

        // กฎข้อ 4: ห้าม ดึก-เช้า (พักไม่พอข้ามวัน)
        if (Number(targetShiftId) === 1) { // แลกเข้าเช้า
            const [prevDay] = await dbPool.query(
                "SELECT Shift_id FROM NurseSchedule WHERE UserID = ? AND Nurse_Date = DATE_SUB(?, INTERVAL 1 DAY)",
                [userId, targetDate]
            );
            for (const row of prevDay) {
                const sId = row.Shift_id || row.shift_id || row.ShiftID;
                if (Number(sId) === 3) return { safe: false, message: "ผิดกฎความปลอดภัย: ห้ามเข้าเวรเช้าต่อจากเวรดึก (พักไม่ถึง 8 ชม.)" };
                if (Number(sId) === 2) return { safe: true, isWarning: true, message: "⚠️ แจ้งเตือน: พักผ่อนน้อยเนื่องจากลงเวรบ่ายต่อเช้า" };
            }
        }

        if (Number(targetShiftId) === 3) { // แลกเข้าดึก
            const [nextDay] = await dbPool.query(
                "SELECT Shift_id FROM NurseSchedule WHERE UserID = ? AND Nurse_Date = DATE_ADD(?, INTERVAL 1 DAY)",
                [userId, targetDate]
            );
            for (const row of nextDay) {
                const sId = row.Shift_id || row.shift_id || row.ShiftID;
                if (Number(sId) === 1) return { safe: false, message: "ผิดกฎความปลอดภัย: คุณมีเวรเช้าในวันพรุ่งนี้ ไม่สามารถลงเวรดึกคืนนี้ได้" };
            }
        }

        // กฎข้อ 5: Rolling 7 Days (ทำงานสูงสุด 6 วันติด)
        if (!ignoreWeeklyLimit) {
            const sixDaysAgo = moment(targetDate).subtract(6, 'days').format('YYYY-MM-DD');
            const [rollingDays] = await dbPool.query(
                "SELECT COUNT(DISTINCT Nurse_Date) as activeDays FROM NurseSchedule WHERE UserID = ? AND Nurse_Date BETWEEN ? AND ?",
                [userId, sixDaysAgo, targetDate]
            );

            if (rollingDays[0].activeDays >= 6) {
                return { safe: false, message: "คุณทำงานติดต่อกันครบ 6 วันแล้ว ต้องมีวันหยุดพักผ่อนอย่างน้อย 1 วัน" };
            }
        }

        return { safe: true, message: "ตรวจสอบแล้วปลอดภัย" };
    } catch (err) {
        console.error("Fatigue Check Error:", err);
        return { safe: false, message: "ระบบตรวจสอบขัดข้อง" };
    }
}

// ==========================================
// 5. API ROUTES
// ==========================================

// Get Roles
app.get('/api/roles', async (req, res) => {
    try {
        const [roles] = await dbPool.query("SELECT RoleID, Role FROM Role WHERE RoleID IN (1, 2)");
        res.json(roles);
    } catch (err) {
        console.error(err); res.status(500).send({ message: 'Error fetching roles' });
    }
});

// Login (Version ปรับปรุงเพื่อรองรับ Force Logout)
app.post('/api/login', loginLimiter, async (req, res) => {
    const ipAddress = req.headers['x-forwarded-for'] || req.socket.remoteAddress || req.ip;
    const schema = Joi.object({
        Email: Joi.string().email().required().messages({
            'string.email': 'รูปแบบอีเมลไม่ถูกต้อง (ต้องมี @ และโดเมน)',
            'string.empty': 'กรุณากรอกอีเมล',
            'any.required': 'กรุณากรอกอีเมล'
        }),
        Password: Joi.string().required().messages({
            'string.empty': 'กรุณากรอกรหัสผ่าน',
            'any.required': 'กรุณากรอกรหัสผ่าน'
        })
    });

    const { error } = schema.validate(req.body);
    if (error) {
        return res.status(400).json({ message: error.details[0].message });
    }

    const { Email, Password } = req.body;

    try {
        // [จุดที่แก้ไข]: เพิ่ม token_version เข้ามาใน SELECT
        const [users] = await dbPool.query(
            "SELECT UserID, FirstName, LastName, PasswordHash, RoleID, ProfileImage, MustChangePassword, token_version FROM User WHERE Email = ?", 
            [Email]
        );

        if (users.length === 0) {
            await logLoginAttempt(dbPool, { Email, IP: ipAddress, Status: 'Failed', FailureReason: 'User not found' });
            return res.status(401).send({ message: 'ไม่พบผู้ใช้ในระบบ' });
        }

        const user = users[0];
        const isMatch = await bcrypt.compare(Password, user.PasswordHash);

        if (!isMatch) {
            await logLoginAttempt(dbPool, { UserID: user.UserID, Email, IP: ipAddress, Status: 'Failed', FailureReason: 'Invalid password' });
            return res.status(401).send({ message: 'รหัสผ่านไม่ถูกต้อง' });
        }

        await dbPool.query("UPDATE User SET Status = 'active' WHERE UserID = ?", [user.UserID]);
        await logLoginAttempt(dbPool, { UserID: user.UserID, Email, IP: ipAddress, Status: 'Success' });
        
        // [จุดที่แก้ไข]: ปั๊มเลข token_version จาก Database ลงไปในกุญแจ (Token) ทุกดอกที่สร้างใหม่
        const token = jwt.sign(
            { 
                userId: user.UserID, 
                roleId: user.RoleID, 
                email: Email,
                token_version: user.token_version // ใส่เลขเวอร์ชันปัจจุบันลงไปใน Payload
            }, 
            JWT_SECRET, 
            { expiresIn: '24h' }
        );

        const { PasswordHash, ...userData } = user;
        userData.Status = 'active';
        
        res.status(200).json({ message: 'ล็อกอินสำเร็จ', status: 'success', token: token, user: userData });

    } catch (err) {
        console.error(err);
        res.status(500).send({ message: 'Server Error' });
    }
});

// Logout
app.post('/logout', async (req, res) => {
    const userId = req.body.userId;
    console.log("--> Logout Request Received:", userId);
    try {
        res.clearCookie('token'); 
        res.json({ message: "Logged out successfully" });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ✅ API สำหรับบังคับเปลี่ยนรหัสผ่าน (ฉบับแก้ไข)
app.post('/api/force-change-password', authenticateToken, async (req, res) => {
    const { userId, newPassword, currentSocketId } = req.body; // <-- รับ currentSocketId เพิ่ม
    const connection = await dbPool.getConnection();

    try {
        await connection.beginTransaction();
        const [userRows] = await connection.query("SELECT * FROM User WHERE UserID = ?", [userId]);
        const user = userRows[0];

        const newVersion = (user.token_version || 0) + 1;
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        
        await connection.query(
            "UPDATE User SET PasswordHash = ?, MustChangePassword = 0, token_version = ? WHERE UserID = ?",
            [hashedPassword, newVersion, userId]
        );

        // สร้าง Token ดอกใหม่ที่มี version ใหม่
        const newToken = jwt.sign(
            { userId: user.UserID, roleId: user.RoleID, email: user.Email, token_version: newVersion }, 
            JWT_SECRET, { expiresIn: '24h' }
        );

        await connection.commit();

        // --- 🛠️ ส่วนที่แก้ไข: เตะเฉพาะเครื่องอื่น ---
        const sessions = onlineUsers.get(String(userId));
        if (sessions) {
            sessions.forEach(socketId => {
                if (socketId !== currentSocketId) {
                    io.to(socketId).emit('force_logout');
                }
            });
        }

        res.json({ success: true, token: newToken });
    } catch (err) {
        await connection.rollback();
        res.status(500).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

// Change Password
app.post('/api/change-password', authenticateToken, async (req, res) => {
    const { oldPassword, newPassword, currentSocketId } = req.body;
    const userId = req.user.userId;

    console.log(`\n🔑 --- Change Password Attempt ---`);
    console.log(`👤 UserID: ${userId}`);
    console.log(`📡 Current SocketID from Client: ${currentSocketId || 'N/A'}`);

    try {
        const [users] = await dbPool.query("SELECT UserID, PasswordHash, Email, RoleID FROM User WHERE UserID = ?", [userId]);
        const user = users[0];

        const isMatch = await bcrypt.compare(oldPassword, user.PasswordHash);
        if (!isMatch) {
            console.log(`❌ Password mismatch for user: ${userId}`);
            return res.status(401).json({ success: false, message: "รหัสผ่านเดิมไม่ถูกต้อง" });
        }

        const newHashedPassword = await bcrypt.hash(newPassword, 10);

        // อัปเดตเลขเวอร์ชันใน DB
        await dbPool.query(
            "UPDATE User SET PasswordHash = ?, token_version = token_version + 1 WHERE UserID = ?", 
            [newHashedPassword, userId]
        );
        console.log(`✅ Database Updated (Token Version increased)`);

        // --- 🛠️ ส่วนส่ง Socket พร้อม Log ตรวจสอบ ---
        const sessions = onlineUsers.get(String(userId));
        if (sessions) {
            console.log(`🌐 Active sessions for this user: ${sessions.size} sessions`);
            sessions.forEach(socketId => {
                if (String(socketId) !== String(currentSocketId)) {
                    console.log(`📡 Sending [force_logout] to: ${socketId} (Other device)`);
                    io.to(socketId).emit('force_logout');
                } else {
                    console.log(`✨ Skipping: ${socketId} (This device)`);
                }
            });
        } else {
            console.log(`ℹ️ No other active sessions found for this user.`);
        }

        // ดึงเลขเวอร์ชันใหม่มาสร้าง Token ดอกใหม่ให้เครื่องปัจจุบัน
        const [updatedUser] = await dbPool.query("SELECT token_version FROM User WHERE UserID = ?", [userId]);
        const newToken = jwt.sign(
            { userId: user.UserID, roleId: user.RoleID, email: user.Email, token_version: updatedUser[0].token_version }, 
            JWT_SECRET, 
            { expiresIn: '24h' }
        );

        res.json({ 
            success: true, 
            message: "เปลี่ยนรหัสผ่านเรียบร้อยแล้ว", 
            token: newToken // ส่งกุญแจใหม่กลับไปเพื่อให้เครื่องนี้ใช้งานต่อได้
        });

    } catch (err) {
        console.error(`🔥 Server Error:`, err);
        res.status(500).json({ success: false, message: "Server Error" });
    }
});
// ✅ เพิ่ม API ตรวจสอบรหัสผ่านเดิม (สำหรับใช้เช็ค Real-time ใน Dashboard)
app.post('/api/check-old-password', authenticateToken, async (req, res) => {
    const { oldPassword } = req.body;
    const userId = req.user.userId; // ดึง UserID จาก Token ที่ล็อกอินอยู่

    try {
        // 1. ดึง PasswordHash ของผู้ใช้จากฐานข้อมูล
        const [users] = await dbPool.query("SELECT PasswordHash FROM User WHERE UserID = ?", [userId]);
        
        if (users.length === 0) {
            return res.status(404).json({ success: false, message: 'User not found' });
        }

        // 2. ใช้ bcrypt เปรียบเทียบรหัสที่พยาบาลพิมพ์มา กับรหัสที่เข้ารหัสไว้ใน DB
        const isMatch = await bcrypt.compare(oldPassword, users[0].PasswordHash);
        
        // 3. ส่งผลลัพธ์กลับไป (true = รหัสถูก, false = รหัสผิด)
        res.json({ valid: isMatch });

    } catch (err) {
        console.error("❌ Check Old Password API Error:", err);
        res.status(500).json({ valid: false, message: "Server error" });
    }
});
// ==========================================
// 1. HELPER: Cloudinary Public ID Extractor (ประกาศที่เดียว)
// ==========================================
function getPublicIdFromUrl(url) {
    if (!url || !url.includes('cloudinary')) return null;
    try {
        // แยกส่วนหลังจาก /upload/
        const parts = url.split('/upload/');
        if (parts.length < 2) return null;
        
        // ตัดเลขเวอร์ชันออก (เช่น v17123456/) และตัดนามสกุลไฟล์ (.jpg, .png)
        const pathAfterUpload = parts[1].replace(/^v\d+\//, ''); 
        const publicId = pathAfterUpload.split('.')[0];
        
        // คืนค่าพร้อมชื่อ Folder (ถ้ามี) เช่น 'autonurseshift-profiles/image_name'
        return publicId;
    } catch (e) {
        console.error("Extract PublicID Error:", e);
        return null;
    }
}

// ==========================================
// 2. API: Update Profile Image (PDPA Compliant)
// ==========================================
app.post('/api/update-profile-image', authenticateToken, upload.single('profileImage'), async (req, res) => {
    const userId = req.user.userId;
    if (!userId || !req.file) {
        return res.status(400).json({ success: false, message: "กรุณาเลือกรูปภาพ" });
    }

    try {
        // 1. ดึง URL รูปเดิมจากฐานข้อมูล
        const [rows] = await dbPool.query("SELECT ProfileImage FROM User WHERE UserID = ?", [userId]);
        const oldImageUrl = rows[0]?.ProfileImage;

        // 2. บันทึก URL ของรูปใหม่ลง Database ก่อน (ถ้า DB พัง รูปใน Cloudinary จะยังไม่ถูกลบ)
        const newImagePath = req.file.path; 
        await dbPool.query("UPDATE User SET ProfileImage = ? WHERE UserID = ?", [newImagePath, userId]);

        // 3. จัดการลบรูปเก่าออกจาก Cloudinary (PDPA Cleanup)
        if (oldImageUrl && oldImageUrl.includes('cloudinary')) {
            const publicId = getPublicIdFromUrl(oldImageUrl);
            if (publicId) {
                // สั่งลบถาวรจาก Cloudinary
                await cloudinary.uploader.destroy(publicId);
                console.log(`[PDPA] Deleted old asset: ${publicId}`);
            }
        }

        res.json({ 
            success: true, 
            message: "อัปเดตรูปโปรไฟล์สำเร็จและทำลายข้อมูลเดิมเรียบร้อย", 
            imagePath: newImagePath 
        });

    } catch (err) {
        console.error("Update Image Error:", err);
        
        // Rollback: หากบันทึก DB ไม่สำเร็จ ให้ลบรูปที่เพิ่งอัปโหลดขึ้นไปใหม่ทิ้งด้วย เพื่อไม่ให้เป็นขยะ
        if (req.file && req.file.path) {
            const newPublicId = getPublicIdFromUrl(req.file.path);
            if (newPublicId) await cloudinary.uploader.destroy(newPublicId);
        }
        
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการบันทึกข้อมูล" });
    }
});

// Forgot Password
app.post('/api/forgot-password', async (req, res) => {
    try {
        const { Email } = req.body;
        console.log("Request Email:", Email); 

        const [users] = await dbPool.query('SELECT UserID, FirstName FROM User WHERE Email = ?', [Email]);
        
        if (users.length === 0) {
            return res.status(404).json({ success: false, message: 'ไม่พบอีเมลนี้ในระบบ' });
        }

        const userId = users[0].UserID;

        // 🔥 [จุดที่ต้องเพิ่ม 1]: สร้างตัวแปร otp ที่ระบบบ่นว่าหาไม่เจอ
        const otp = generateOTP(); 
        const expiresAt = getThaiTimeInMySQLFormat(10); // หมดอายุใน 10 นาที

        // 🔥 [จุดที่ต้องเพิ่ม 2]: บันทึก OTP ลงฐานข้อมูล (ไม่งั้นหน้า verify-otp จะตรวจไม่ผ่าน)
        // 🔥 แก้ไขคำสั่ง INSERT ในหน้า forgot-password
        await dbPool.query(
            "INSERT INTO Password_reset_otp (UserID, otp_code, created_at, expires_at, is_used) VALUES (?, ?, ?, ?, 0)",
            [
                userId, 
                otp, 
                getThaiTimeInMySQLFormat(), // ✅ เพิ่มเวลาที่สร้าง ณ ตอนนี้
                expiresAt
            ]
        );
        const info = await transporter.sendMail({
            from: '"AUTONURSESHIFT System" <ใช้อีเมลจริงที่ยืนยันกับ Brevo แล้ว>', // 
            to: Email,
            subject: '🔑 รหัสยืนยันตัวตน (OTP) สำหรับรีเซ็ตรหัสผ่าน',
            html: `
            <div style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; max-width: 600px; margin: auto; border: 1px solid #e0e0e0; border-radius: 8px; overflow: hidden; background-color: #ffffff;">
                <div style="background-color: #007bff; padding: 30px; text-align: center;">
                    <h1 style="color: #ffffff; margin: 0; font-size: 28px; letter-spacing: 2px; font-weight: bold;">AUTONURSESHIFT</h1>
                </div>
                
                <div style="padding: 40px 20px; text-align: center;">
                    <h2 style="color: #333333; margin-bottom: 10px; font-size: 22px;">รหัสยืนยันตัวตน (OTP)</h2>
                    
                    <div style="margin-bottom: 25px; padding: 10px; background-color: #fcfcfc; border-radius: 5px;">
                        <p style="margin: 5px 0; color: #4a5568; font-size: 15px;">สวัสดีคุณ <strong>${users[0].FirstName}</strong></p>
                        <p style="margin: 5px 0; color: #718096; font-size: 14px;">(บัญชี: ${Email})</p>
                    </div>

                    <p style="color: #666666; font-size: 16px; line-height: 1.5; margin-bottom: 30px;">
                        ระบบได้รับคำขอให้รีเซ็ตรหัสผ่านสำหรับบัญชีของคุณ<br>
                        โปรดใช้รหัส OTP ด้านล่างนี้เพื่อดำเนินการต่อ:
                    </p>
                    
                    <div style="display: inline-block; padding: 20px 60px; border: 2px dashed #007bff; border-radius: 10px; background-color: #f8fbff; margin-bottom: 30px;">
                        <span style="font-size: 48px; font-weight: bold; color: #007bff; letter-spacing: 10px;">${otp}</span>
                    </div>
                    
                    <div style="color: #d9534f; font-size: 16px; margin-top: 10px;">
                        <span style="font-size: 20px;">⚠️</span> รหัสนี้จะหมดอายุภายใน 10 นาที
                    </div>
                </div>
                
                <div style="padding: 20px; text-align: center; border-top: 1px solid #f0f0f0; background-color: #fafafa;">
                    <p style="margin: 0; color: #999999; font-size: 12px;">หากคุณไม่ได้ร้องขอรหัสนี้ โปรดแจ้งให้หัวหน้าพยาบาลทราบทันที</p>
                    <p style="margin: 5px 0 0; color: #bdc3c7; font-size: 11px;">© 2026 AUTONURSESHIFT System</p>
                </div>
            </div>` 
        });

        return res.status(200).json({ success: true, message: 'ส่ง OTP เรียบร้อย' });

    } catch (err) {
        console.error("Forgot PW Error Detail:", err);
        return res.status(500).json({ 
            success: false, 
            message: "ขออภัย ระบบส่งเมลขัดข้อง", 
            error: err.message 
        });
    }
});
// Verify OTP
app.post("/verify-otp", async (req, res) => {
    const { email, otp } = req.body;
    try {
        const [users] = await dbPool.query("SELECT UserID FROM User WHERE Email = ?", [email]);
        if (users.length === 0) return res.status(404).json({ success: false, message: "ไม่พบอีเมลผู้ใช้งานในระบบ" });

        const userId = users[0].UserID;
        const [otps] = await dbPool.query("SELECT * FROM Password_reset_otp WHERE UserID = ? ORDER BY otp_id DESC LIMIT 1", [userId]);

        if (otps.length === 0) return res.status(400).json({ success: false, message: "ไม่พบข้อมูลการขอ OTP" });

        const otpData = otps[0];
        if (String(otpData.otp_code) !== String(otp)) return res.status(400).json({ success: false, message: "รหัส OTP ไม่ถูกต้อง" });
        if (otpData.is_used === 1) return res.status(400).json({ success: false, message: "รหัส OTP นี้ถูกใช้งานไปแล้ว" });
        
        const currentTime = getThaiTimeInMySQLFormat(0); 
        const expireTime = new Date(otpData.expires_at).toLocaleString('sv-SE'); 

        if (currentTime > expireTime) return res.status(400).json({ success: false, message: "รหัส OTP หมดอายุแล้ว" });

        res.json({ success: true, message: "OTP ถูกต้อง ยืนยันตัวตนสำเร็จ" });

    } catch (err) { 
        console.error("Verify OTP Error:", err);
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

// Reset Password
// Reset Password (ฉบับปรับปรุง: สั่งเตะทุกเครื่องออกเพื่อความปลอดภัย)
app.post("/api/reset-password", async (req, res) => {
    const { email, newPassword, otp } = req.body;
    const connection = await dbPool.getConnection();

    try {
        await connection.beginTransaction();

        // 1. ตรวจสอบผู้ใช้
        const [users] = await connection.query("SELECT UserID FROM User WHERE Email = ?", [email]);
        if (users.length === 0) {
            connection.release();
            return res.status(404).json({ success: false, message: "ไม่พบผู้ใช้งาน" });
        }
        const userId = users[0].UserID;

        // 2. เข้ารหัสรหัสผ่านใหม่
        const hashedPassword = await bcrypt.hash(newPassword, 10);

        // 3. อัปเดตรหัสผ่านใหม่ และ ดีดเลข token_version ขึ้น 1 (เพื่อให้ Token เก่าทั้งหมดใช้ไม่ได้)
        await connection.query(
            "UPDATE User SET PasswordHash = ?, token_version = token_version + 1 WHERE UserID = ?", 
            [hashedPassword, userId]
        );

        // 4. ทำเครื่องหมายว่า OTP ถูกใช้แล้ว
        await connection.query(
            "UPDATE Password_reset_otp SET is_used = 1 WHERE UserID = ? AND otp_code = ?", 
            [userId, otp]
        );

        await connection.commit();

        // 5. 🚨 สั่งเตะทุกเครื่องผ่าน Socket.io (Force Logout)
        const sessions = onlineUsers.get(String(userId));
        if (sessions) {
            sessions.forEach(socketId => {
                io.to(socketId).emit('force_logout');
            });
        }

        res.json({ 
            success: true, 
            message: "เปลี่ยนรหัสผ่านสำเร็จ ระบบได้ทำการออกจากระบบในอุปกรณ์อื่นทั้งหมดแล้ว" 
        });

    } catch (err) {
        if (connection) await connection.rollback();
        console.error("Reset Password Error:", err);
        res.status(500).json({ success: false, message: "Server Error" });
    } finally {
        if (connection) connection.release();
    }
});

// Import Users
app.post('/api/admin/import-users', authenticateToken, uploadExcel.single('file'), async (req, res) => {
    const connection = await dbPool.getConnection();

    try {
        if (req.user.roleId !== 1) {
            return res.status(403).json({ success: false, message: 'Access Denied: เฉพาะหัวหน้าพยาบาลเท่านั้น' });
        }

        if (!req.file) return res.status(400).json({ success: false, message: 'กรุณาเลือกไฟล์ Excel' });

        const workbook = xlsx.read(req.file.buffer, { type: 'buffer' });
        const data = xlsx.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);

        await connection.beginTransaction();

        let successCount = 0;
        const thaiTime = getThaiTimeInMySQLFormat();

        for (const [index, row] of data.entries()) {
            const email = row['Email']?.trim();
            const firstName = row['FirstName']?.trim();
            const lastName = row['LastName']?.trim() || '';
            const roleId = row['RoleID'] || 2;

            if (!email || !firstName) throw new Error(`แถวที่ ${index + 2}: ข้อมูลไม่ครบ`);

            const rawPassword = generateRandomPassword(8);
            const hashedPassword = await bcrypt.hash(rawPassword, 10);

            // 1. Insert User
            const [userResult] = await connection.query(
                `INSERT INTO User (Email, PasswordHash, FirstName, LastName, RoleID, Status, MustChangePassword, CreatedAt) 
                 VALUES (?, ?, ?, ?, ?, 'active', 1, ?)`,
                [email, hashedPassword, firstName, lastName, roleId, thaiTime]
            );

            // 2. สร้าง Notification ต้อนรับ (พยาบาลจะเห็นเมื่อเข้าแอปครั้งแรก)
            await connection.query(
                `INSERT INTO Notifications (UserID, Title, Message, Type, CreatedAt) 
                 VALUES (?, '🎊 ยินดีต้อนรับเข้าสู่ระบบ', 'บัญชีของคุณถูกสร้างเรียบร้อยแล้ว กรุณาเปลี่ยนรหัสผ่านเพื่อความปลอดภัย', 'system', ?)`,
                [userResult.insertId, thaiTime]
            );

            // 3. ส่ง Email (ทำแบบ Async ไม่ต้องรอ await เพื่อความเร็ว)
            const mailOptions = {
                from: '"AUTONURSESHIFT System" <ใช้อีเมลจริงที่ยืนยันกับ Brevo แล้ว>',
                to: email,
                subject: '🔑 ข้อมูลเข้าใช้งานระบบ AUTONURSESHIFT',
                html: `
                    <div style="font-family: sans-serif; border: 1px solid #eee; padding: 20px; border-radius: 10px;">
                        <h2 style="color: #007bff;">ยินดีต้อนรับคุณ ${firstName}</h2>
                        <p>หัวหน้างานได้เพิ่มคุณเข้าสู่ระบบ <b>AUTONURSESHIFT</b> เรียบร้อยแล้ว</p>
                        <p><b>ชื่อผู้ใช้ (Email):</b> ${email}</p>
                        <p><b>รหัสผ่านชั่วคราว:</b> <code style="background: #eee; padding: 4px 8px;">${rawPassword}</code></p>
                        <br>
                        <a href={ลิงก์ frontend}/login.html" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">เข้าสู่ระบบที่นี่</a>
                        <p style="color: #d9534f; margin-top: 20px;">* คุณต้องเปลี่ยนรหัสผ่านทันทีหลังเข้าสู่ระบบครั้งแรก</p>  //
                    </div>
                `
            };
            transporter.sendMail(mailOptions).catch(err => console.error(`Mail Error (${email}):`, err.message));

            successCount++;
        }

        await connection.commit();
        res.json({ success: true, message: `นำเข้าข้อมูลสำเร็จ ${successCount} ท่าน` });

    } catch (err) {
        await connection.rollback();
        res.status(500).json({ success: false, message: 'การนำเข้าล้มเหลว: ' + err.message });
    } finally {
        connection.release();
    }
});

app.get('/api/admin/available-months', authenticateToken, async (req, res) => {
    try {
        const sql = `
            SELECT DISTINCT YEAR(Nurse_Date) as year, MONTH(Nurse_Date) as month
            FROM NurseSchedule
            ORDER BY year DESC, month DESC
        `;
        const [rows] = await dbPool.query(sql);
        if (rows.length === 0) {
            const now = new Date();
            rows.push({ year: now.getFullYear(), month: now.getMonth() + 1 });
        }
        res.json({ success: true, months: rows });
    } catch (err) {
        console.error("Error fetching available months:", err);
        res.status(500).json({ success: false, message: 'Server Error' });
    }
});

// ==========================================
// 6. DASHBOARD & ADMIN APIs
// ==========================================
app.post('/api/dashboard-summary', authenticateToken, async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ message: "UserID required" });

    try {
        const [[user]] = await dbPool.query("SELECT FirstName, LastName, ProfileImage FROM User WHERE UserID = ?", [userId]);
        if (!user) return res.status(404).json({ message: "User not found" });

        const [monthRes, weekRes, exchangeRes, tradeRes, upcomingRes] = await Promise.all([
            // 1. นับเวรเดือนนี้: เช็คทั้งเดือนและปีปัจจุบัน เพื่อไม่ให้ข้อมูลปีเก่ามาปน
            dbPool.query(`SELECT COUNT(*) as count FROM NurseSchedule WHERE UserID = ? AND MONTH(Nurse_Date) = MONTH(CURRENT_DATE()) AND YEAR(Nurse_Date) = YEAR(CURRENT_DATE())`, [userId]),
            
            // 2. นับเวรสัปดาห์นี้: ใช้ YEARWEEK โหมด 1 (เริ่มจันทร์-อาทิตย์) 
            // เป็นวิธีที่ปลอดภัยที่สุด ไม่ว่าจะเป็นช่วงรอยต่อเดือน ตัวเลขจะตรงตามสัปดาห์ปฏิทินเสมอ
           dbPool.query(`SELECT COUNT(*) as count FROM NurseSchedule WHERE UserID = ? AND YEARWEEK(Nurse_Date, 1) = YEARWEEK(CURRENT_DATE(), 1)AND MONTH(Nurse_Date) = MONTH(CURRENT_DATE())`, [userId]),

            // 3. นับรายการรอแลกเปลี่ยน
            dbPool.query(`SELECT COUNT(*) as count FROM Shift_Exchange WHERE requester_id = ? AND status = 'pending'`, [userId]),
            
            // 4. นับรายการรออนุมัติขาย
            dbPool.query(`SELECT COUNT(*) as count FROM ShiftTransaction WHERE SellerID = ? AND Status = 'Pending'`, [userId]),
            
            // 5. ดึงเวรที่กำลังจะมาถึง 2 รายการแรก
            dbPool.query(`SELECT NS.Nurse_Date, S.ShiftName, S.StartTime, S.EndTime FROM NurseSchedule NS JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE NS.UserID = ? AND NS.Nurse_Date >= CURRENT_DATE() ORDER BY NS.Nurse_Date ASC LIMIT 2`, [userId])
        ]);

        res.json({
            success: true,
            userData: { fullName: `${user.FirstName} ${user.LastName}`, profileImage: user.ProfileImage },
            stats: { 
                totalMonth: monthRes[0][0].count, 
                totalWeek: weekRes[0][0].count, 
                pendingExchange: exchangeRes[0][0].count, 
                pendingTrade: tradeRes[0][0].count 
            },
            upcomingShifts: upcomingRes[0] 
        });
    } catch (err) { 
        console.error("Dashboard Error:", err); 
        res.status(500).json({ message: "Server Error" }); 
    }
});

app.get('/api/check-constraint-window', authenticateToken, async (req, res) => {
    try {
        const [status] = await dbPool.query(
            "SELECT SettingValue FROM SystemSettings WHERE SettingKey = 'SystemStatus'"
        );
        const [deadline] = await dbPool.query(
            "SELECT SettingValue FROM SystemSettings WHERE SettingKey = 'DeadlineDate'"
        );
        const isOpen = status.length > 0 && status[0].SettingValue === 'Open';
        const deadlineValue = deadline.length > 0 
            ? new Date(deadline[0].SettingValue).toLocaleString('sv-SE', { timeZone: 'Asia/Bangkok' })
            : null;
        const response = {
            isOpen: isOpen,
            deadline: deadlineValue
        };
        console.log("API response:", response);
        res.json(response);
    } catch (err) {
        console.error(err);
        res.json({
            isOpen: false,
            deadline: null
        });
    }
});
app.post('/api/admin/toggle-window', authenticateToken, async (req, res) => { 
    try { 
        await dbPool.query(`
            INSERT INTO SystemSettings (SettingKey, SettingValue) 
            VALUES ('SystemStatus', ?) 
            ON DUPLICATE KEY UPDATE SettingValue = VALUES(SettingValue)
        `, [req.body.status]); 
        
        res.json({ success: true }); 
    } catch (err) { 
        console.error("Toggle Window Error:", err);
        res.status(500).json({ success: false }); 
    } 
});
app.post('/api/notifications/mark-all-read', authenticateToken, async (req, res) => {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ success: false, message: "Missing UserID" });
    try {
        await dbPool.query("UPDATE Notifications SET IsRead = 1 WHERE UserID = ? AND IsRead = 0", [userId]);
        res.json({ success: true, message: "ทำเครื่องหมายว่าอ่านแล้วทั้งหมดเรียบร้อย" });
        sendRealTimeNotification(userId, { type: 'mark_read_success' });
    } catch (err) {
        console.error("Mark Read Error:", err);
        res.status(500).json({ success: false, message: "Server Error" });
    }
});

app.get('/api/notifications/unread-count/:userId', authenticateToken, async (req, res) => {
    try {
        const userId = req.params.userId;
        const userRole = req.user.roleId; 

        const [systemNotis] = await dbPool.query("SELECT COUNT(*) as count FROM Notifications WHERE UserID = ? AND IsRead = 0", [userId]);
        let pendingActionCount = 0;

        if (userRole === 2) {
            const [swapReqs] = await dbPool.query("SELECT COUNT(*) as count FROM Shift_Exchange WHERE responder_id = ? AND status = 'pending'", [userId]);
            const [buyReqs] = await dbPool.query("SELECT COUNT(*) as count FROM ShiftTransaction WHERE SellerID = ? AND Status = 'Pending_Seller'", [userId]);
            pendingActionCount = (swapReqs[0].count || 0) + (buyReqs[0].count || 0);
        }

        res.json({ 
            success: true, 
            count: (systemNotis[0].count || 0) + pendingActionCount 
        });

    } catch (err) { 
        console.error("Unread Count API Error:", err);
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

app.get('/api/admin/pending-counts', authenticateToken, async (req, res) => {
    if (req.user.roleId != 1) return res.status(403).json({ success: false, message: 'Access Denied: Head Nurse Only' });
    try {
        const [swap] = await dbPool.query("SELECT COUNT(*) as count FROM Shift_Exchange WHERE status = 'accepted'");
        const [trade] = await dbPool.query("SELECT COUNT(*) as count FROM ShiftTransaction WHERE Status = 'Pending_HeadNurse'");
        res.json({ 
            success: true, 
            total: (swap[0].count || 0) + (trade[0].count || 0), 
            swapCount: swap[0].count || 0, 
            tradeCount: trade[0].count || 0 
        });
    } catch (err) { 
        console.error("Pending Counts Error:", err);
        res.status(500).json({ success: false }); 
    }
});

app.get('/api/admin/get-settings', authenticateToken, async (req, res) => {
    try {
        const [rows] = await dbPool.query('SELECT * FROM SystemSettings');
        const settings = {};
        
        rows.forEach(r => {
            // แปลงข้อมูลกลับจาก String เป็น Object สำหรับ Quotas
            if (r.SettingKey === 'WeekdayQuotas') settings.weekdayQuotas = JSON.parse(r.SettingValue);
            if (r.SettingKey === 'WeekendQuotas') settings.weekendQuotas = JSON.parse(r.SettingValue);
            if (r.SettingKey === 'HolidayQuotas') settings.holidayQuotas = JSON.parse(r.SettingValue);
            
            // ข้อมูล Deadline และของเดิม (Backward Compatibility)
            if (r.SettingKey === 'DeadlineDate') settings.deadline = r.SettingValue;
            if (r.SettingKey === 'SystemStatus') settings.SystemStatus = r.SettingValue;
            
            // คงของเดิมไว้เผื่อฟังก์ชันอื่นยังเรียกใช้ morning, afternoon, night แบบตรงๆ (ดึงจาก weekday เป็นหลัก)
            if (r.SettingKey === 'QuotaMorning') settings.morning = r.SettingValue;
            if (r.SettingKey === 'QuotaAfternoon') settings.afternoon = r.SettingValue;
            if (r.SettingKey === 'QuotaNight') settings.night = r.SettingValue;
        });

        res.json({ success: true, settings });
    } catch (err) {
        console.error("Get Settings Error:", err);
        res.status(500).json({ success: false });
    }
});
app.post('/api/admin/save-settings', authenticateToken, async (req, res) => {
    // 1. รับค่า status เพิ่มเติมจากเดิม
    const { weekdayQuotas, weekendQuotas, holidayQuotas, deadline, status} = req.body;

    // 2. เพิ่มสถานะ Open/Closed เข้าไปในรายการที่จะบันทึก
    const settingsData = [
        { key: 'WeekdayQuotas', value: JSON.stringify(weekdayQuotas) },
        { key: 'WeekendQuotas', value: JSON.stringify(weekendQuotas) },
        { key: 'HolidayQuotas', value: JSON.stringify(holidayQuotas) },
        { key: 'DeadlineDate', value: deadline },
        { key: 'SystemStatus', value: status } // บันทึกว่า 'Open' หรือ 'Closed'
    ];

    try {
        for (const item of settingsData) {
            if (item.value !== undefined && item.value !== null) {
                await dbPool.query(`
                    INSERT INTO SystemSettings (SettingKey, SettingValue) 
                    VALUES (?, ?) 
                    ON DUPLICATE KEY UPDATE SettingValue = VALUES(SettingValue)
                `, [item.key, item.value]);
            }
        }
        res.json({ success: true, message: "บันทึกการตั้งค่าและสถานะระบบเรียบร้อยแล้ว" });
    } catch (err) {
        console.error("Save Settings Error:", err);
        res.status(500).json({ success: false, message: "Server Error" });
    }
});
// ==========================================
// 7. SCHEDULE & SWAP SYSTEM
// ==========================================
app.post('/api/monthly-schedule', authenticateToken, async (req, res) => {
    const { userId, month, year } = req.body;
    if (!userId) return res.status(400).json({ message: "Data missing" });
    try {
        const sql = `SELECT DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, S.ShiftName, S.StartTime, S.EndTime, S.Shift_id, NS.ScheduleID FROM NurseSchedule NS JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE NS.UserID = ? AND MONTH(NS.Nurse_Date) = ? AND YEAR(NS.Nurse_Date) = ? ORDER BY NS.Nurse_Date ASC`;
        const [shifts] = await dbPool.query(sql, [userId, month, year]);
        res.json({ success: true, shifts });
    } catch (err) { res.status(500).json({ message: "Server Error" }); }
});

app.post('/api/set-constraints', authenticateToken, async (req, res) => {
    const { userId, settingPeriod, daysOffMinimum, fixedDaysOff, preferences } = req.body;
    try {
        // ✅ 1. มั่นใจว่า fixedDaysOff เป็น String ก่อนลง DB
        const fixedDaysString = Array.isArray(fixedDaysOff) ? JSON.stringify(fixedDaysOff) : (fixedDaysOff || "[]");

        // ✅ 2. ปรับชื่อคอลัมน์ให้ตรงเป๊ะกับ Workbench (DaysOffMin, FixedDaysOff)
        const sql = `
            INSERT INTO Constraints (
                UserID, SettingPeriod, Constraint_Date, DaysOffMin, FixedDaysOff, 
                PrefMorning, PrefAfternoon, PrefNight, Reason, CreatedAt
            ) 
            VALUES (?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE 
                Constraint_Date = NOW(),
                DaysOffMin = VALUES(DaysOffMin), 
                FixedDaysOff = VALUES(FixedDaysOff),
                PrefMorning = VALUES(PrefMorning),
                PrefAfternoon = VALUES(PrefAfternoon),
                PrefNight = VALUES(PrefNight),
                Reason = VALUES(Reason)
        `;

        await dbPool.query(sql, [
            userId, 
            settingPeriod, // ต้องเป็นรูปแบบ 'YYYY-MM-01'
            daysOffMinimum, 
            fixedDaysString, 
            preferences.Morning, 
            preferences.Afternoon, 
            preferences.Night, 
            'User Preference', 
            getThaiTimeInMySQLFormat()
        ]);

        res.json({ success: true, message: "บันทึกข้อมูลเรียบร้อยแล้ว" });
    } catch (err) {
        console.error("Database Save Error:", err);
        res.status(500).json({ success: false, message: "DB Error: " + err.message });
    }
});

app.get('/api/my-constraint-status', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.userId;
        // คำนวณรอบเดือนถัดไปให้ตรงกับใน DB (2026-02-01)
        const nextMonthStr = moment().add(1, 'month').startOf('month').format('YYYY-MM-DD');

        const sql = `SELECT ConstraintID FROM Constraints WHERE UserID = ? AND SettingPeriod = ? LIMIT 1`;
        const [rows] = await dbPool.query(sql, [userId, nextMonthStr]);

        if (rows.length > 0) {
            // ถ้าเจอข้อมูลในรอบเดือนนั้น ให้ส่งสถานะ 'ส่งแล้ว'
            res.json({ success: true, status: 'ส่งแล้ว', ConstraintID: rows[0].ConstraintID });
        } else {
            res.json({ success: true, status: 'ยังไม่ส่ง' });
        }
    } catch (err) {
        console.error("Status Check Error:", err);
        res.status(500).json({ success: false, message: "Server Error" });
    }
});

app.post('/api/posts/create', authenticateToken, async (req, res) => {
    const connection = await dbPool.getConnection();
    try {
        const { userId, scheduleId, desiredDate, note } = req.body;
        await connection.beginTransaction();

        // 1. เช็คสิทธิ์ความเป็นเจ้าของเวร และล็อคข้อมูลไว้ป้องกันการประกาศซ้ำ
        const [scheduleRows] = await connection.query(
            "SELECT UserID FROM NurseSchedule WHERE ScheduleID = ? FOR UPDATE", [scheduleId]
        );
        
        if (scheduleRows.length === 0) throw new Error('ไม่พบข้อมูลตารางเวร');
        if (scheduleRows[0].UserID !== userId) throw new Error('คุณไม่ใช่เจ้าของเวรนี้');

        // 2. ตรวจสอบโพสต์ที่ยังเปิดอยู่ (Status = 'Open') ของเวรนี้
        const [duplicateRows] = await connection.query(
            "SELECT ExchangePostID FROM ExchangePost WHERE ScheduleID = ? AND Status = 'Open' FOR UPDATE", [scheduleId]
        );
        
        if (duplicateRows.length > 0) throw new Error('เวรนี้มีการตั้งประกาศแลกไว้อยู่แล้ว');

        // 3. บันทึกประกาศ
        const sql = `INSERT INTO ExchangePost (UserID, ScheduleID, DesiredShiftDate, Message, Status, CreatedAt) VALUES (?, ?, ?, ?, 'Open', ?)`;
        const [result] = await connection.query(sql, [userId, scheduleId, desiredDate || null, note || null, getThaiTimeInMySQLFormat()]);

        await connection.commit(); // ✅ สำเร็จทั้งหมด
        res.status(201).json({ success: true, message: 'สร้างประกาศแลกเวรเรียบร้อยแล้ว', postId: result.insertId });

    } catch (err) {
        await connection.rollback(); // ❌ ยกเลิกหากพังหรือซ้ำ
        res.status(400).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

app.post('/api/full-schedule', authenticateToken, async (req, res) => {
    try {
        const { month, year } = req.body;
        if (!month || !year) return res.status(400).json({ success: false, message: "กรุณาระบุเดือนและปี" });
        const sql = `SELECT NS.ScheduleID, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, U.UserID, U.FirstName, U.LastName, U.ProfileImage, U.RoleID, S.Shift_id, S.ShiftName, S.StartTime, S.EndTime FROM NurseSchedule NS JOIN User U ON NS.UserID = U.UserID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE MONTH(NS.Nurse_Date) = ? AND YEAR(NS.Nurse_Date) = ? ORDER BY NS.Nurse_Date ASC, S.StartTime ASC, U.FirstName ASC`;
        const [shifts] = await dbPool.query(sql, [month, year]);
        res.json({ success: true, shifts: shifts });
    } catch (err) { console.error("Full Schedule Error:", err); res.status(500).json({ success: false, message: "Server Error: " + err.message }); }
});

app.post('/api/swaps/search', authenticateToken, async (req, res) => {
    const { date, shiftId, requesterId } = req.body;
    const userRole = req.user.roleId; 

    if (!date) return res.status(400).json({ success: false, message: "กรุณาระบุวันที่ต้องการค้นหา" });
    
    try {
        let sql = `
            SELECT U.UserID, U.FirstName, U.LastName, U.ProfileImage, NS.ScheduleID, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, 
            S.ShiftName, S.Shift_id, EP.ExchangePostID, EP.Message,
            CASE WHEN EP.ExchangePostID IS NOT NULL THEN 'Posted' ELSE 'Normal' END as SwapStatus
            FROM NurseSchedule NS 
            JOIN User U ON NS.UserID = U.UserID 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            LEFT JOIN ExchangePost EP ON NS.ScheduleID = EP.ScheduleID AND EP.Status = 'Open'
            WHERE NS.Nurse_Date = ? AND U.UserID != ?

            -- 1. กรองสิทธิ์: หัวหน้า (Role 1) เห็นเฉพาะเวรเช้า (Shift 1)
            AND (? != 1 OR S.Shift_id = 1)

            -- 2. กรองเวรซ้ำ: ซ่อนกะที่เรามีอยู่แล้วในวันนั้น (เฉพาะเวรที่ไม่ได้ประกาศขาย/แลกออก)
            AND NOT EXISTS (
                SELECT 1 FROM NurseSchedule myNS 
                LEFT JOIN PostSell PS_Check ON myNS.ScheduleID = PS_Check.ScheduleID AND PS_Check.Status = 'Open'
                LEFT JOIN Shift_Exchange SE_Check ON myNS.ScheduleID = SE_Check.requester_schedule_id AND SE_Check.status IN ('pending', 'accepted')
                WHERE myNS.UserID = ? AND myNS.Nurse_Date = NS.Nurse_Date AND myNS.Shift_id = NS.Shift_id
                AND PS_Check.PostSellID IS NULL 
                AND SE_Check.exchange_id IS NULL
            )

            -- 3. กรองกฎ บ่าย-ดึก: ถ้าเรามีเวรบ่ายวันนี้ ห้ามเห็นเวรดึกเพื่อนวันนี้
            AND NOT (S.Shift_id = 3 AND EXISTS (
                SELECT 1 FROM NurseSchedule sameDay WHERE sameDay.UserID = ? AND sameDay.Nurse_Date = NS.Nurse_Date AND sameDay.Shift_id = 2
            ))

            -- 4. กรองกฎ ดึก-เช้า: ถ้าเรามีเวรดึกเมื่อวาน ห้ามเห็นเวรเช้าวันนี้
            AND NOT (S.Shift_id = 1 AND EXISTS (
                SELECT 1 FROM NurseSchedule prevDay 
                WHERE prevDay.UserID = ? AND prevDay.Nurse_Date = DATE_SUB(NS.Nurse_Date, INTERVAL 1 DAY) AND prevDay.Shift_id = 3
            ))

            -- 5. กรองกฎ ดึก-เช้า (อนาคต): ถ้าเรามีเวรเช้าพรุ่งนี้ ห้ามเห็นเวรดึกวันนี้
            AND NOT (S.Shift_id = 3 AND EXISTS (
                SELECT 1 FROM NurseSchedule nextDay 
                WHERE nextDay.UserID = ? AND nextDay.Nurse_Date = DATE_ADD(NS.Nurse_Date, INTERVAL 1 DAY) AND nextDay.Shift_id = 1
            ))

            -- 6. กรองโควตา (แก้ไข): นับเฉพาะเวรที่ 'ว่างจริง' ไม่นับเวรที่กำลังประกาศขายหรือรอแลกออก
            AND (
                SELECT COUNT(NS_Main.ScheduleID) 
                FROM NurseSchedule NS_Main
                LEFT JOIN PostSell PS_Count ON NS_Main.ScheduleID = PS_Count.ScheduleID AND PS_Count.Status = 'Open'
                LEFT JOIN Shift_Exchange SE_Count ON NS_Main.ScheduleID = SE_Count.requester_schedule_id AND SE_Count.status IN ('pending', 'accepted')
                WHERE NS_Main.UserID = ? 
                AND NS_Main.Nurse_Date = NS.Nurse_Date
                AND PS_Count.PostSellID IS NULL 
                AND SE_Count.exchange_id IS NULL
            ) < 2
        `;
        
        // ลำดับ Params: [date, requesterId, userRole, requesterId, requesterId, requesterId, requesterId, requesterId]
        const params = [date, requesterId, userRole, requesterId, requesterId, requesterId, requesterId, requesterId];
        
        if (shiftId) { 
            sql += " AND S.Shift_id = ? "; 
            params.push(shiftId); 
        }
        
        sql += " ORDER BY SwapStatus DESC, U.FirstName ASC";
        const [results] = await dbPool.query(sql, params);
        res.json({ success: true, results });
    } catch (err) { 
        console.error("Search Swap Error:", err); 
        res.status(500).json({ success: false, message: "DB Error" }); 
    }
});
app.post('/api/swaps/send-request', authenticateToken, async (req, res) => {
    const connection = await dbPool.getConnection();

    try {
        const { requesterId, requesterScheduleId, postId, targetScheduleId, reason } = req.body;
        if (!requesterId || !requesterScheduleId) {
            return res.status(400).json({ success: false, message: 'ข้อมูลฝั่งคนขอไม่ครบถ้วน' });
        }

        await connection.beginTransaction();

        let responderId = null;
        let responderScheduleId = null;

        // --- 1. ดึงข้อมูลผู้รับ ---
        if (postId) {
            const [postData] = await connection.query("SELECT UserID, ScheduleID FROM ExchangePost WHERE ExchangePostID = ? FOR UPDATE", [postId]);
            if (postData.length === 0) throw new Error('ไม่พบประกาศนี้ในระบบ');
            responderId = postData[0].UserID;
            responderScheduleId = postData[0].ScheduleID;
        } else if (targetScheduleId) {
            const [scheduleData] = await connection.query("SELECT UserID, ScheduleID FROM NurseSchedule WHERE ScheduleID = ? FOR UPDATE", [targetScheduleId]);
            if (scheduleData.length === 0) throw new Error('ไม่พบเวรที่ต้องการแลก');
            responderId = scheduleData[0].UserID;
            responderScheduleId = scheduleData[0].ScheduleID;
        } else {
            throw new Error('ระบุข้อมูลไม่ครบ');
        }

        if (responderId == requesterId) throw new Error('คุณจะแลกเวรกับตัวเองไม่ได้');

        // --- 2. Fatigue Checks & Duplicate Checks ---
        const [reqShiftInfo] = await connection.query("SELECT Nurse_Date, Shift_id FROM NurseSchedule WHERE ScheduleID = ?", [requesterScheduleId]);
        const [resShiftInfo] = await connection.query("SELECT Nurse_Date, Shift_id FROM NurseSchedule WHERE ScheduleID = ?", [responderScheduleId]);
        if (reqShiftInfo.length === 0 || resShiftInfo.length === 0) throw new Error("ไม่พบข้อมูลรายละเอียดเวร");

        const requesterShift = reqShiftInfo[0]; 
        const responderShift = resShiftInfo[0]; 

        const safetyRequester = await checkFatigueStatus(connection, requesterId, responderShift.Nurse_Date, responderShift.Shift_id);
        if (!safetyRequester.safe) throw new Error(`คุณไม่สามารถแลกเวรนี้ได้: ${safetyRequester.message}`);

        const safetyResponder = await checkFatigueStatus(connection, responderId, requesterShift.Nurse_Date, requesterShift.Shift_id);
        if (!safetyResponder.safe) throw new Error(`เพื่อนจะผิดกฎความปลอดภัยหากแลกเวรนี้: ${safetyResponder.message}`);

        const [existing] = await connection.query(
            "SELECT exchange_id FROM Shift_Exchange WHERE requester_schedule_id = ? AND responder_schedule_id = ? AND status = 'pending'", 
            [requesterScheduleId, responderScheduleId]
        );
        if (existing.length > 0) throw new Error('คุณเคยส่งคำขอสำหรับเวรนี้ไปแล้ว');

        // --- 3. บันทึกคำขอแลกเวร (แก้บั๊ก ExchangePostID และป้องกันชื่อตัวแปรซ้ำ) ---
        const sqlExchange = `INSERT INTO Shift_Exchange (requester_id, requester_schedule_id, responder_id, responder_schedule_id, status, reason, ExchangePostID, created_at) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)`;
        const [insertResult] = await connection.query(sqlExchange, [
            requesterId, 
            requesterScheduleId, 
            responderId, 
            responderScheduleId, 
            reason, 
            postId || null, 
            getThaiTimeInMySQLFormat()
        ]);
        
        const finalExchangeId = insertResult.insertId;

        // --- 4. บันทึกการแจ้งเตือนลง DB ---
        const notiMsg = `มีคำขอแลกเวรวันที่ ${moment(responderShift.Nurse_Date).format('DD/MM')} เข้ามาใหม่ โปรดตรวจสอบ`;
        await connection.query(`
            INSERT INTO Notifications (UserID, Title, Message, Type, RelatedDate, RefID, CreatedAt) 
            VALUES (?, '✨ มีคำขอแลกเวรใหม่', ?, 'alert', ?, ?, ?)
        `, [responderId, notiMsg, responderShift.Nurse_Date, finalExchangeId, getThaiTimeInMySQLFormat()]);

        // --- 5. ดึงข้อมูล Email เพื่อเตรียมส่ง ---
        const [responderEmail] = await connection.query("SELECT Email, FirstName FROM User WHERE UserID = ?", [responderId]);

        await connection.commit();

        // --- 6. ส่ง Real-time Notification ---
        sendRealTimeNotification(responderId, {
            title: '✨ มีคำขอแลกเวรใหม่',
            message: notiMsg,
            type: 'alert',
            refId: finalExchangeId,
            relatedDate: responderShift.Nurse_Date
        });

        if (responderEmail.length > 0) {
            const mailOptions = {
                from: '"AUTONURSESHIFT System" <ใช้อีเมลจริงที่ยืนยันกับ Brevo แล้ว>',
                to: responderEmail[0].Email,
                subject: '✨ มีคำขอแลกเวรใหม่รอการตอบกลับ',
                html: `<p>สวัสดีคุณ ${responderEmail[0].FirstName},</p><p>มีเพื่อนพยาบาลส่งคำขอแลกเวรมาถึงคุณ โปรดตรวจสอบได้ในระบบ</p>`
            };
            transporter.sendMail(mailOptions).catch(err => console.error("Email Error:", err.message));
        }

        res.json({ success: true, message: 'ส่งคำขอแลกเวรสำเร็จและแจ้งเตือนเพื่อนเรียบร้อยแล้ว' });

    } catch (err) {
        await connection.rollback();
        console.error("Swap Request Error:", err.message);
        res.status(400).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

app.post('/api/swaps/respond', authenticateToken, async (req, res) => {
    const { swapId, action, responderId } = req.body;
    const connection = await dbPool.getConnection(); 

    try {
        await connection.beginTransaction(); 

        // 1. ตรวจสอบสิทธิ์และล็อคแถวข้อมูลไว้ก่อน
        const [check] = await connection.query(
            "SELECT * FROM Shift_Exchange WHERE exchange_id = ? AND responder_id = ? AND status = 'pending' FOR UPDATE", 
            [swapId, responderId]
        );
        
        if (check.length === 0) throw new Error("ไม่มีสิทธิ์ดำเนินการ หรือสถานะไม่ถูกต้อง");
        const swapData = check[0]; 

        if (action === 'approve') {
            // 2. อัปเดตสถานะการแลก
            await connection.query("UPDATE Shift_Exchange SET status = 'accepted' WHERE exchange_id = ?", [swapId]);
            
            // 3. แจ้งเตือนคนขอ (Requester)
            await connection.query(`
                INSERT INTO Notifications (UserID, Title, Message, Type, CreatedAt) 
                VALUES (?, '✅ เพื่อนตอบรับแล้ว', 'เพื่อนยอมรับการแลกเวรแล้ว (รอหัวหน้าอนุมัติขั้นสุดท้าย)', 'alert', ?)
            `, [swapData.requester_id, getThaiTimeInMySQLFormat()]);

            // 4. แจ้งเตือนหัวหน้าพยาบาลทุกคน
            const [admins] = await connection.query("SELECT UserID FROM User WHERE RoleID = 1");
            for (const admin of admins) {
                await connection.query(`
                    INSERT INTO Notifications (UserID, Title, Message, Type, RefID, CreatedAt) 
                    VALUES (?, '📝 งานทะเบียน: รออนุมัติแลกเวร', 'มีการตกลงแลกเวรใหม่ รอการตรวจสอบจากท่าน', 'alert', ?, ?)
                `, [admin.UserID, swapId, getThaiTimeInMySQLFormat()])
            }
        } else {
            // กรณีปฏิเสธ
            await connection.query("UPDATE Shift_Exchange SET status = 'rejected' WHERE exchange_id = ?", [swapId]);
            await connection.query(`
                INSERT INTO Notifications (UserID, Title, Message, Type, CreatedAt) 
                VALUES (?, '❌ เพื่อนปฏิเสธ', 'เพื่อนไม่สะดวกแลกเวรกับคุณในครั้งนี้', 'alert', ?)
            `, [swapData.requester_id, getThaiTimeInMySQLFormat()]);
        }

        await connection.commit(); 
        sendRealTimeNotification(swapData.requester_id, { title: 'แจ้งเตือนสถานะการแลกเวร', type: 'alert' });

        res.json({ success: true, message: "ดำเนินการเรียบร้อย" });

    } catch (err) {
        await connection.rollback(); // ❌ ยกเลิกทั้งหมดถ้าพังแม้แต่นิดเดียว
        res.status(400).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

app.get('/api/notifications/all/:userId', authenticateToken, async (req, res) => {
    try {
        const userId = req.params.userId;
        const sql = `
            SELECT 
                N.NotiID AS id, 
                N.Title, 
                N.Message AS info, 
                N.Type, 
                N.RefID, -- 🔥 สำคัญมากสำหรับสร้างปุ่ม
                DATE_FORMAT(N.RelatedDate, '%Y-%m-%d') AS ShiftDate, 
                N.RelatedShift AS ShiftName, 
                N.CreatedAt AS created_at,
                U.FirstName, 
                U.LastName
            FROM Notifications N 
            LEFT JOIN User U ON N.UserID = U.UserID -- เพื่อแสดงชื่อเจ้าของรายการ
            WHERE N.UserID = ? 
            ORDER BY N.CreatedAt DESC
        `;
        const [notis] = await dbPool.query(sql, [userId]);
        res.json({ success: true, notifications: notis });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});
app.get('/api/admin/swaps/pending', authenticateToken, async (req, res) => {
    try {
        // แก้ไขบรรทัด WHERE SE.status จาก 'pending' เป็น 'accepted'
        const sql = `
            SELECT SE.exchange_id, SE.reason, SE.created_at, 
                   ReqU.FirstName AS ReqName, ReqU.LastName AS ReqLast, 
                   ReqShift.ShiftName AS ReqShift, ReqNS.Nurse_Date AS ReqDate, 
                   ResU.FirstName AS ResName, ResU.LastName AS ResLast, 
                   ResShift.ShiftName AS ResShift, ResNS.Nurse_Date AS ResDate 
            FROM Shift_Exchange SE 
            JOIN User ReqU ON SE.requester_id = ReqU.UserID 
            JOIN NurseSchedule ReqNS ON SE.requester_schedule_id = ReqNS.ScheduleID 
            JOIN Shift ReqShift ON ReqNS.Shift_id = ReqShift.Shift_id 
            JOIN User ResU ON SE.responder_id = ResU.UserID 
            JOIN NurseSchedule ResNS ON SE.responder_schedule_id = ResNS.ScheduleID 
            JOIN Shift ResShift ON ResNS.Shift_id = ResShift.Shift_id 
            WHERE SE.status = 'accepted'  -- *** จุดที่ต้องแก้ ***
            ORDER BY SE.created_at ASC`;

        const [results] = await dbPool.query(sql);
        res.json({ success: true, results });
    } catch (err) { 
        console.error(err); 
        res.status(500).json({ success: false, message: "DB Error" }); 
    }
});

app.post('/api/admin/swaps/action', authenticateToken, async (req, res) => {
    const { swapId, action, adminId } = req.body;
    const connection = await dbPool.getConnection();
    
    try {
        await connection.beginTransaction();

        // 1. ดึงข้อมูลรายการแลกเวรและใช้ FOR UPDATE เพื่อล็อค Row
        const [swaps] = await connection.query(`
            SELECT se.*, 
                   ns1.Nurse_Date as Date1, ns1.Shift_id as Shift1, 
                   ns2.Nurse_Date as Date2, ns2.Shift_id as Shift2 
            FROM Shift_Exchange se 
            JOIN NurseSchedule ns1 ON se.requester_schedule_id = ns1.ScheduleID 
            JOIN NurseSchedule ns2 ON se.responder_schedule_id = ns2.ScheduleID 
            WHERE se.exchange_id = ? AND se.status = 'accepted' FOR UPDATE`, [swapId]);

        if (swaps.length === 0) {
            throw new Error("ไม่พบรายการแลกเวรที่รออนุมัติ หรือรายการนี้ถูกดำเนินการไปแล้ว");
        }
        
        const swap = swaps[0];

        if (action === 'approve') {
            // 2. 🔥 FINAL SAFETY CHECK: ตรวจสอบกฎความปลอดภัยอีกครั้ง
            const safety1 = await checkFatigueStatus(connection, swap.requester_id, swap.Date2, swap.Shift2, { excludeScheduleId: swap.requester_schedule_id });
            const safety2 = await checkFatigueStatus(connection, swap.responder_id, swap.Date1, swap.Shift1, { excludeScheduleId: swap.responder_schedule_id });

            if (!safety1.safe || !safety2.safe) {
                const reason = !safety1.safe ? `คนขอ: ${safety1.message}` : `คนรับ: ${safety2.message}`;
                throw new Error(`ไม่สามารถอนุมัติได้เนื่องจากผิดกฎความปลอดภัย: ${reason}`);
            }

            // 3. สลับเจ้าของเวรในตารางหลัก
            await connection.query("UPDATE NurseSchedule SET UserID = ? WHERE ScheduleID = ?", [swap.responder_id, swap.requester_schedule_id]);
            await connection.query("UPDATE NurseSchedule SET UserID = ? WHERE ScheduleID = ?", [swap.requester_id, swap.responder_schedule_id]);

            // 4. 🔥 CLEANUP GHOST DATA: ลบประกาศที่เกี่ยวข้องทั้งหมด
            await connection.query("UPDATE ExchangePost SET Status = 'Closed' WHERE ScheduleID IN (?, ?)", [swap.requester_schedule_id, swap.responder_schedule_id]);
            await connection.query("UPDATE PostSell SET Status = 'Cancelled' WHERE ScheduleID IN (?, ?)", [swap.requester_schedule_id, swap.responder_schedule_id]);
            await connection.query(`
                UPDATE ShiftTransaction 
                SET Status = 'Rejected' 
                WHERE ScheduleID IN (?, ?) AND Status IN ('Pending_Seller', 'Pending_HeadNurse')`, 
                [swap.requester_schedule_id, swap.responder_schedule_id]
            );

            // 5. AUTO-REJECT: ปฏิเสธรายการแลกเวรอื่นที่ซ้อนกัน
            await connection.query(`
                UPDATE Shift_Exchange 
                SET status = 'rejected', reason = 'เวรนี้ถูกสลับเปลี่ยนเจ้าของผ่านรายการอื่นไปแล้ว' 
                WHERE exchange_id != ? 
                AND (requester_schedule_id IN (?, ?) OR responder_schedule_id IN (?, ?))
                AND status IN ('pending', 'accepted')`, 
                [swapId, swap.requester_schedule_id, swap.responder_schedule_id, swap.requester_schedule_id, swap.responder_schedule_id]
            );

            // 6. อัปเดตสถานะรายการเป็น Approved
            await connection.query("UPDATE Shift_Exchange SET status = 'approved', approved_by = ? WHERE exchange_id = ?", [adminId, swapId]);

        } else if (action === 'reject') {
            await connection.query("UPDATE Shift_Exchange SET status = 'rejected', approved_by = ? WHERE exchange_id = ?", [adminId, swapId]);
        }

        await connection.commit();

        // 7. 🚀 SAFE NOTIFICATION (Sanitize String เพื่อป้องกัน Frontend พัง)
        const dateStr = `${moment(swap.Date1).format('DD/MM')} และ ${moment(swap.Date2).format('DD/MM')}`;
        const msgSuccess = `หัวหน้าอนุมัติการแลกเวรวันที่ ${dateStr} เรียบร้อยแล้ว`.replace(/['"]/g, ""); // ล้างเครื่องหมายที่อาจทำให้ JS พัง
        
        const notifyPayload = { title: '✅ การแลกเวรสำเร็จ', message: msgSuccess, type: 'system' };
        
        sendRealTimeNotification(swap.requester_id, notifyPayload);
        sendRealTimeNotification(swap.responder_id, notifyPayload);

        res.json({ success: true, message: action === 'approve' ? "อนุมัติการแลกเวรสำเร็จ" : "ปฏิเสธการแลกเวรเรียบร้อย" });

    } catch (err) {
        if (connection) await connection.rollback();
        console.error("❌ Swap Action Error:", err.message);
        res.status(400).json({ success: false, message: err.message });
    } finally {
        if (connection) connection.release();
    }
});

app.get('/api/swaps/history/:userId', authenticateToken, async (req, res) => {
    try {
        const userId = req.params.userId;
        const sql = `SELECT SE.exchange_id, SE.status, DATE_FORMAT(SE.created_at, '%Y-%m-%dT%H:%i:%s') as created_at, SE.reason, ResU.FirstName AS PartnerName, ResU.LastName AS PartnerLastName, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as ShiftDate, S.ShiftName, CASE WHEN SE.requester_id = ? THEN 'Sent Request' ELSE 'Incoming Request' END as Direction FROM Shift_Exchange SE JOIN User ResU ON (SE.responder_id = ResU.UserID OR SE.requester_id = ResU.UserID) JOIN NurseSchedule NS ON SE.responder_schedule_id = NS.ScheduleID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE (SE.requester_id = ? OR SE.responder_id = ?) AND ResU.UserID != ? ORDER BY SE.created_at DESC`;
        const [results] = await dbPool.query(sql, [userId, userId, userId, userId]);
        res.json({ success: true, results });
    } catch (err) { console.error("History Error:", err); res.status(500).json({ success: false, message: "Server Error" }); }
});

app.get('/api/posts/user/:userId', authenticateToken, async (req, res) => {
    try {
        const sql = `SELECT EP.ExchangePostID as PostID, EP.ScheduleID, EP.DesiredShiftDate as DesiredDate, EP.Message as Note, EP.CreatedAt as Created_At, S.ShiftName, NS.Nurse_Date FROM ExchangePost EP JOIN NurseSchedule NS ON EP.ScheduleID = NS.ScheduleID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE EP.UserID = ? AND EP.Status = 'Open' ORDER BY EP.CreatedAt DESC`;
        
        const [results] = await dbPool.query(sql, [req.params.userId]);
        res.json({ success: true, results });
    } catch (err) { console.error(err); res.status(500).json({ success: false, message: "DB Error" }); }
});



app.delete('/api/posts/delete/:postId', authenticateToken, async (req, res) => {
    const { postId } = req.params;
    const userId = req.user.userId;

    try {
        // 1. ดึงข้อมูลโพสต์เพื่อหา ScheduleID และตรวจสอบความเป็นเจ้าของ
        const [post] = await dbPool.query(
            "SELECT ScheduleID, UserID FROM ExchangePost WHERE ExchangePostID = ?", 
            [postId]
        );

        if (post.length === 0) {
            return res.status(404).json({ success: false, message: "ไม่พบประกาศที่ต้องการลบ" });
        }

        // ตรวจสอบว่าเป็นเจ้าของโพสต์จริงไหม
        if (post[0].UserID !== userId) {
            return res.status(403).json({ success: false, message: "คุณไม่มีสิทธิ์ลบประกาศนี้" });
        }

        const scheduleId = post[0].ScheduleID;

        // 2. ตรวจสอบว่าเวรนี้ (ScheduleID) ติดสถานะกำลังแลกเปลี่ยนหรือไม่
        // เช็คในตาราง Shift_Exchange ว่ามีรายการที่สถานะเป็น 'pending' หรือ 'approved' (รอหัวหน้า) อยู่ไหม
        const [activeSwaps] = await dbPool.query(
            `SELECT exchange_id FROM Shift_Exchange 
             WHERE (requester_schedule_id = ? OR responder_schedule_id = ?) 
             AND status IN ('pending', 'approved') 
             LIMIT 1`,
            [scheduleId, scheduleId]
        );

        if (activeSwaps.length > 0) {
            return res.status(400).json({ 
                success: false, 
                message: "ไม่สามารถลบได้ เนื่องจากเวรนี้กำลังอยู่ในกระบวนการแลกเปลี่ยน" 
            });
        }

        // 3. ถ้าตรวจสอบผ่านแล้ว ให้ทำการลบ
        await dbPool.query("DELETE FROM ExchangePost WHERE ExchangePostID = ?", [postId]);
        
        res.json({ success: true, message: "ลบประกาศสำเร็จ" });

    } catch (err) {
        console.error("Delete Post Error:", err);
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดที่เซิร์ฟเวอร์" });
    }
});

app.get('/api/swaps/check-status/:scheduleId', async (req, res) => {
    const { scheduleId } = req.params;

    try {
        // ใช้ชื่อตาราง Shift_Exchange และคอลัมน์ให้ตรงกับ Schema จริงของคุณ
        const sql = `
            SELECT exchange_id, status 
            FROM Shift_Exchange 
            WHERE (requester_schedule_id = ? OR responder_schedule_id = ?) 
            AND status IN ('pending', 'approved')
            LIMIT 1
        `;

        // ตรวจสอบว่าใช้ db หรือ dbPool
        const [rows] = await dbPool.query(sql, [scheduleId, scheduleId]);

        if (rows.length > 0) {
            return res.json({ 
                success: true, 
                isPending: true, 
                status: rows[0].status,
                message: "เวรนี้กำลังอยู่ในกระบวนการแลกเปลี่ยน" 
            });
        }

        return res.json({ 
            success: true, 
            isPending: false, 
            message: "เวรนี้สามารถแก้ไขได้" 
        });

    } catch (error) {
        console.error("Check status error:", error);
        res.status(500).json({ success: false, message: "Server error" });
    }
});

app.get('/api/schedule/my-future-shifts/:userId', authenticateToken, async (req, res) => {
    try {
        const userId = req.params.userId;
        const targetScheduleId = req.query.targetScheduleId; 
        const userRole = req.user.roleId;

        // 1. ดึงเวรในอนาคตทั้งหมดที่ "ว่าง" หรือ "แค่ลงประกาศขายไว้"
        const sqlMyShifts = `
            SELECT NS.ScheduleID, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, S.ShiftName, S.Shift_id,
                   PS.PostSellID, SE_Req.exchange_id as PendingExchangeID
            FROM NurseSchedule NS 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            LEFT JOIN PostSell PS ON NS.ScheduleID = PS.ScheduleID AND PS.Status = 'Open'
            LEFT JOIN Shift_Exchange SE_Req ON NS.ScheduleID = SE_Req.requester_schedule_id AND SE_Req.status IN ('pending', 'accepted')
            LEFT JOIN ShiftTransaction ST ON NS.ScheduleID = ST.ScheduleID AND ST.Status IN ('Pending_Seller', 'Pending_HeadNurse')
            WHERE NS.UserID = ? 
            AND NS.Nurse_Date >= CURRENT_DATE()
            AND SE_Req.exchange_id IS NULL
            AND ST.TransactionID IS NULL
            ORDER BY NS.Nurse_Date ASC
        `;
        const [myShifts] = await dbPool.query(sqlMyShifts, [userId]);

        if (!targetScheduleId) {
            return res.json({ success: true, results: myShifts });
        }

        // 2. ดึงข้อมูลเวรของเพื่อน (คนที่จะไปแลกด้วย)
        const [targetInfo] = await dbPool.query(
            "SELECT UserID, Nurse_Date, Shift_id FROM NurseSchedule WHERE ScheduleID = ?", 
            [targetScheduleId]
        );
        if (targetInfo.length === 0) return res.json({ success: true, results: [] });

        const targetUser = targetInfo[0];

        // 3. กรองเวรที่ "แสดงผลได้" (ยอมให้เวรที่มี Warning ปรากฏตัว)
        const filteredShifts = [];

        for (const myShift of myShifts) {
            // เช็คความปลอดภัยฝั่งเพื่อน (ถ้าแลกไปแล้วเพื่อนไหวไหม)
            const safetyForFriend = await checkFatigueStatus(
                dbPool, 
                targetUser.UserID, 
                myShift.Nurse_Date, 
                myShift.Shift_id, 
                { excludeScheduleId: targetScheduleId } 
            );

            // เช็คความปลอดภัยฝั่งเรา (ถ้าเอาเวรเพื่อนมาเราไหวไหม)
            const safetyForMe = await checkFatigueStatus(
                dbPool, 
                userId, 
                targetUser.Nurse_Date, 
                targetUser.Shift_id, 
                { excludeScheduleId: myShift.ScheduleID } 
            );

            // --- จุดที่แก้ไข: เงื่อนไขการยอมให้เวรปรากฏใน Dropdown ---
            // ยอมรับถ้า: (ปลอดภัย 100%) หรือ (มีแค่คำเตือนแต่ระบบยังถือว่า safe)
            if (safetyForFriend.safe && safetyForMe.safe) {
                filteredShifts.push({
                    ...myShift,
                    isSelling: myShift.PostSellID !== null,
                    // ส่งคำเตือนพ่วงไปให้หน้าบ้านโชว์ Alert ถ้าจำเป็น
                    warningMessage: safetyForMe.isWarning ? safetyForMe.message : (safetyForFriend.isWarning ? "เพื่อนอาจมีความเสี่ยง: " + safetyForFriend.message : null)
                });
            }
        }

        res.json({ success: true, results: filteredShifts });

    } catch (err) { 
        console.error("Error fetching future shifts:", err); 
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการตรวจสอบตารางเวร" }); 
    }
});

// ==========================================
// 8. STATISTICS & MARKET SYSTEM
// ==========================================
app.post('/api/my-stats', authenticateToken, async (req, res) => {
    const { userId, year } = req.body;
    
    // ตรวจสอบความครบถ้วนของข้อมูล
    if (!userId || !year) {
        return res.status(400).json({ success: false, message: 'ข้อมูลไม่ครบถ้วน' });
    }
    
    let connection;
    try {
        connection = await dbPool.getConnection();

        // 1. นับสถิติเวรจริงแยกตามกะจาก NurseSchedule
        const [workRes] = await connection.query(`
            SELECT 
                MONTH(Nurse_Date) as month, 
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN Shift_id = 1 THEN 1 ELSE 0 END), 0) as morning,
                COALESCE(SUM(CASE WHEN Shift_id = 2 THEN 1 ELSE 0 END), 0) as afternoon,
                COALESCE(SUM(CASE WHEN Shift_id = 3 THEN 1 ELSE 0 END), 0) as night
            FROM NurseSchedule 
            WHERE UserID = ? AND YEAR(Nurse_Date) = ?
            GROUP BY MONTH(Nurse_Date)`, [userId, year]);

        // 2. นับการแลกเวร: เช็คทั้งฝั่ง requester_id OR responder_id (เฉพาะที่ approved)
        const [exchangeRes] = await connection.query(`
            SELECT MONTH(created_at) as month, COUNT(*) as count
            FROM Shift_Exchange 
            WHERE (requester_id = ? OR responder_id = ?) 
            AND status = 'approved' 
            AND YEAR(created_at) = ?
            GROUP BY MONTH(created_at)`, [userId, userId, year]);

        // 3. นับการซื้อขาย: เช็คทั้งฝั่ง SellerID OR BuyerID (เฉพาะที่ Completed)
        const [tradeRes] = await connection.query(`
            SELECT MONTH(CreatedAt) as month, COUNT(*) as count
            FROM ShiftTransaction 
            WHERE (SellerID = ? OR BuyerID = ?) 
            AND Status = 'Completed' 
            AND YEAR(CreatedAt) = ?
            GROUP BY MONTH(CreatedAt)`, [userId, userId, year]);

        // 4. ประกอบข้อมูล 12 เดือน ดัก NULL เป็น 0 และรวมทั้งสองฝั่ง
        const monthlyDetails = [];
        for (let m = 1; m <= 12; m++) {
            const daysInMonth = new Date(year, m, 0).getDate();
            const w = workRes.find(r => r.month === m) || { total: 0, morning: 0, afternoon: 0, night: 0 };
            const e = exchangeRes.find(r => r.month === m) || { count: 0 };
            const t = tradeRes.find(r => r.month === m) || { count: 0 };

            monthlyDetails.push({
                month: m,
                total: w.total || 0,
                morning: w.morning || 0,
                afternoon: w.afternoon || 0,
                night: w.night || 0,
                totalSwaps: e.count || 0, // ยอดรวมแลกเวรทั้งในฐานะคนขอและคนรับ
                totalTrades: t.count || 0, // ยอดรวมซื้อขายทั้งในฐานะคนซื้อและคนขาย
                offDays: Math.max(0, daysInMonth - (w.total || 0))
            });
        }

        // 5. สรุปยอดรวมภาพรวมทั้งปี
        const summary = monthlyDetails.reduce((acc, curr) => ({
            shifts: acc.shifts + curr.total,
            swaps: acc.swaps + curr.totalSwaps,
            trades: acc.trades + curr.totalTrades
        }), { shifts: 0, swaps: 0, trades: 0 });

        res.json({
            success: true,
            data: {
                year: parseInt(year),
                totalShifts: summary.shifts,
                totalHours: summary.shifts * 8,
                totalSwaps: summary.swaps,
                totalTrades: summary.trades,
                monthlyDetails: monthlyDetails
            }
        });

    } catch (err) {
        console.error("❌ API Error:", err.message);
        res.status(500).json({ success: false, message: "Internal Server Error", error: err.message });
    } finally {
        if (connection) connection.release();
    }
});

app.put('/api/posts/update', authenticateToken, async (req, res) => {
    try {
        const { postId, desiredDate, note, scheduleId, userId } = req.body;
        
        if (!postId) return res.status(400).json({ success: false, message: 'ไม่พบรหัสโพสต์' });

        // ✅ เพิ่มการเช็ค: ถ้า scheduleId เป็น "undefined" หรือไม่มีค่า ให้แจ้งเตือน
        if (!scheduleId || scheduleId === 'undefined') {
            return res.status(400).json({ success: false, message: 'ข้อมูลเวรไม่ถูกต้อง (ScheduleID missing)' });
        }

        // 1. ตรวจสอบโพสต์ซ้ำ
        const [dup] = await dbPool.query(
            "SELECT ExchangePostID FROM ExchangePost WHERE ScheduleID = ? AND Status = 'Open' AND ExchangePostID != ?",
            [scheduleId, postId]
        );
        if (dup.length > 0) {
            return res.status(400).json({ success: false, message: 'เวรนี้มีการประกาศแลกไปแล้ว' });
        }

        // 2. อัปเดตข้อมูล
        const dateValue = (desiredDate && desiredDate !== "") ? desiredDate : null;
        const sql = `UPDATE ExchangePost SET DesiredShiftDate = ?, Message = ?, ScheduleID = ? WHERE ExchangePostID = ? AND UserID = ?`;
        
        const [result] = await dbPool.query(sql, [dateValue, note, scheduleId, postId, userId]);

        if (result.affectedRows === 0) return res.status(404).json({ success: false, message: 'ไม่พบโพสต์ หรือคุณไม่มีสิทธิ์แก้ไข' });

        res.json({ success: true, message: 'บันทึกการเปลี่ยนแปลงเรียบร้อย' });

    } catch (err) { 
        console.error("Update Post Error:", err); 
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

app.get('/api/swaps/incoming/:userId', authenticateToken, async (req, res) => {
    const userId = req.params.userId;
    const sql = `SELECT se.exchange_id AS SwapID, se.requester_id AS RequesterID, CONCAT(u.FirstName, ' ', u.LastName) AS RequesterName, u.ProfileImage, se.requester_schedule_id AS RequesterScheduleID, s_req.Nurse_Date AS RequesterDate, sh_req.ShiftName AS RequesterShift, se.responder_schedule_id AS TargetScheduleID, s_target.Nurse_Date AS TargetDate, sh_target.ShiftName AS TargetShift, se.status AS Status, se.created_at AS Created_At FROM Shift_Exchange se JOIN User u ON se.requester_id = u.UserID JOIN NurseSchedule s_req ON se.requester_schedule_id = s_req.ScheduleID JOIN Shift sh_req ON s_req.Shift_id = sh_req.Shift_id JOIN NurseSchedule s_target ON se.responder_schedule_id = s_target.ScheduleID JOIN Shift sh_target ON s_target.Shift_id = sh_target.Shift_id WHERE se.responder_id = ? AND se.status = 'pending' ORDER BY se.created_at DESC`;
    try {
        const [results] = await dbPool.query(sql, [userId]);
        res.json({ success: true, results: results });
    } catch (err) { console.error("Incoming Swaps Error:", err); res.status(500).json({ success: false, message: 'Server Error' }); }
});

app.post('/api/sell-shift', authenticateToken, async (req, res) => {
    // 1. ดึงท่อเชื่อมต่อพิเศษเพื่อเริ่มทำ Transaction
    const connection = await dbPool.getConnection();

    const schema = Joi.object({
        userId: Joi.number().required().messages({
            'number.base': 'UserID ต้องเป็นตัวเลข',
            'any.required': 'ไม่พบข้อมูลผู้ใช้งาน'
        }),
        scheduleId: Joi.number().required().messages({
            'number.base': 'รหัสตารางเวรไม่ถูกต้อง',
            'any.required': 'กรุณาระบุเวรที่ต้องการขาย'
        }),
        price: Joi.number().min(0).required().messages({
            'number.base': 'ราคาต้องเป็นตัวเลขเท่านั้น',
            'number.min': 'ราคาต้องไม่ต่ำกว่า 0 บาท',
            'any.required': 'กรุณาระบุราคาขาย'
        }),
        message: Joi.string().max(255).allow('', null).messages({
            'string.max': 'หมายเหตุต้องยาวไม่เกิน 255 ตัวอักษร'
        })
    });

    const { error } = schema.validate(req.body);
    if (error) {
        connection.release(); // คืนท่อทันทีถ้าข้อมูลผิด Format
        return res.status(400).json({ success: false, message: error.details[0].message });
    }

    const { userId, scheduleId, price, message } = req.body;

    try {
        // 2. เริ่มต้น Transaction (พื้นที่บันทึกชั่วคราว)
        await connection.beginTransaction();

        // 3. ตรวจสอบสิทธิ์ความเป็นเจ้าของเวร และ "ล็อคแถว" (FOR UPDATE) 
        // ป้องกันคนอื่นมาแอบแก้หรือขายซ้ำในจังหวะเดียวกัน
        const [scheduleRows] = await connection.query(
            "SELECT UserID FROM NurseSchedule WHERE ScheduleID = ? FOR UPDATE", 
            [scheduleId]
        );

        if (scheduleRows.length === 0) {
            throw new Error('ไม่พบข้อมูลตารางเวรที่ระบุ');
        }
        if (scheduleRows[0].UserID !== userId) {
            throw new Error('คุณไม่ใช่เจ้าของเวรนี้ ไม่สามารถลงประกาศขายได้');
        }

        // 4. ตรวจสอบว่าเวรนี้ถูกลงประกาศขาย (Open) ไว้ก่อนหน้าแล้วหรือยัง
        const [existingPost] = await connection.query(
            "SELECT PostSellID FROM PostSell WHERE ScheduleID = ? AND Status = 'Open' FOR UPDATE", 
            [scheduleId]
        );

        if (existingPost.length > 0) {
            throw new Error('เวรนี้ถูกลงประกาศขายอยู่ในระบบแล้ว');
        }

        // 5. บันทึกประกาศขายใหม่
        const sql = `INSERT INTO PostSell (UserID, ScheduleID, Price, Message, Status, CreatedAT) VALUES (?, ?, ?, ?, 'Open', ?)`;
        const [result] = await connection.query(sql, [
            userId, 
            scheduleId, 
            price, 
            message || null, 
            getThaiTimeInMySQLFormat()
        ]);

        // 6. ถ้าทำงานถึงตรงนี้แสดงว่าผ่านหมด สั่ง Commit เพื่อบันทึกลง Database จริง
        await connection.commit();

        res.json({ 
            success: true, 
            message: 'ลงประกาศขายเวรในตลาดเรียบร้อยแล้ว', 
            id: result.insertId 
        });

    } catch (err) {
        // 7. หากมี Error (จาก throw Error หรือระบบพัง) สั่งยกเลิกทุกอย่าง
        await connection.rollback();
        
        console.error("❌ Sell Shift Rollback Error:", err.message);
        res.status(400).json({ 
            success: false, 
            message: err.message || 'เกิดข้อผิดพลาดในการลงประกาศขาย' 
        });

    } finally {
        // 8. คืน Connection กลับ Pool เสมอ (สำคัญมาก!)
        connection.release();
    }
});

app.get('/api/market/shifts', authenticateToken, async (req, res) => {
    const filterType = req.query.type;
    const currentUserId = req.query.userId; 

    try {
        let sql = `
            SELECT PS.PostSellID, PS.Price, PS.Message as ConditionText, PS.CreatedAT, 
                   PS.UserID as SellerID, U.FirstName, U.LastName, U.ProfileImage, 
                   DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, 
                   S.ShiftName, S.StartTime, S.EndTime, 
                   CASE WHEN PS.Price LIKE '%ด่วน%' THEN 1 ELSE 0 END as IsUrgent 
            FROM PostSell PS 
            JOIN NurseSchedule NS ON PS.ScheduleID = NS.ScheduleID 
            JOIN User U ON PS.UserID = U.UserID 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            WHERE PS.Status = 'Open' 
            AND PS.UserID != ? 
            
            -- 1. ซ่อนเวรที่ 'กะตรงกัน' เป๊ะๆ (ป้องกัน Error Duplicate Entry)
            AND NOT EXISTS (
                SELECT 1 FROM NurseSchedule myNS 
                WHERE myNS.UserID = ? 
                AND myNS.Nurse_Date = NS.Nurse_Date 
                AND myNS.Shift_id = NS.Shift_id
            )

            -- 2. ซ่อน 'ทุกกะ' ในวันที่เรามีเวรครบ 2 กะแล้ว (ป้องกันความล้า)
            AND (
                SELECT COUNT(*) FROM NurseSchedule countNS 
                WHERE countNS.UserID = ? 
                AND countNS.Nurse_Date = NS.Nurse_Date
            ) < 2
        `;

        // ใส่ currentUserId ลงใน Parameter ทั้ง 3 จุด
        const params = [currentUserId, currentUserId, currentUserId];

        // ส่วน Filter ประเภทเวร เช้า/บ่าย/ดึก (ถ้ามี)
        if (filterType && filterType !== 'all') {
            let likeTerm = filterType === 'morning' ? '%เช้า%' : filterType === 'afternoon' ? '%บ่าย%' : '%ดึก%';
            sql += ` AND S.ShiftName LIKE ?`;
            params.push(likeTerm);
        }

        sql += ` ORDER BY IsUrgent DESC, NS.Nurse_Date ASC`;

        const [results] = await dbPool.query(sql, params);
        
        // ... (ส่วนการ map formattedResults ส่งกลับเหมือนเดิม) ...
        const formattedResults = results.map(row => ({
            id: row.PostSellID,
            user_name: `${row.FirstName} ${row.LastName}`,
            shift_date: row.Nurse_Date,
            shift_time_label: row.ShiftName,
            condition: row.Price,
            is_urgent: row.IsUrgent === 1
        }));

        res.json(formattedResults);

    } catch (err) {
        console.error("Market Fetch Error:", err);
        res.status(500).json({ success: false, message: "Server Error" });
    }
});

app.post('/api/market/request-trade', authenticateToken, async (req, res) => {
    const { postSellId, buyerId } = req.body;
    const connection = await dbPool.getConnection();

    try {
        await connection.beginTransaction();

        // 1. ดึงข้อมูลโพสต์และล็อค Row เพื่อป้องกัน Race Condition
        const [posts] = await connection.query(
            `SELECT PS.PostSellID, PS.UserID AS SellerID, PS.ScheduleID, PS.Price, 
                    DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date,
                    NS.Shift_id
             FROM PostSell PS 
             JOIN NurseSchedule NS ON PS.ScheduleID = NS.ScheduleID 
             WHERE PS.PostSellID = ? AND PS.Status = 'Open' FOR UPDATE`, 
            [postSellId]
        );

        if (posts.length === 0) {
            throw new Error("โพสต์นี้ถูกซื้อไปแล้วหรือถูกยกเลิก");
        }
        
        const post = posts[0];

        // 2. เช็คว่ามีคนอื่นกำลังกดซื้อค้างไว้หรือไม่
        const [existingPending] = await connection.query(
            `SELECT TransactionID FROM ShiftTransaction 
             WHERE PostSellID = ? AND Status IN ('Pending_Seller', 'Pending_HeadNurse') FOR UPDATE`,
            [postSellId]
        );

        if (existingPending.length > 0) {
            throw new Error("มีพยาบาลท่านอื่นกำลังทำรายการซื้อเวรนี้อยู่");
        }

        // 3. 🛡️ [ทางสายกลาง] เช็ค Fatigue แต่ไม่บล็อกการทำงาน
        const safetyCheck = await checkFatigueStatus(connection, buyerId, post.Nurse_Date, post.Shift_id);
        let fatigueWarning = null;
        if (!safetyCheck.safe) {
            // สร้างข้อความเตือนเพื่อไปแสดงผล แต่ไม่ throw Error
            fatigueWarning = `⚠️ ข้อควรระวัง: ${safetyCheck.message}`;
        }

        // 4. บันทึกรายการซื้อขายลงตาราง ShiftTransaction
        const [result] = await connection.query(
            `INSERT INTO ShiftTransaction (PostSellID, ScheduleID, SellerID, BuyerID, Price, Status, CreatedAt) 
             VALUES (?, ?, ?, ?, ?, 'Pending_Seller', ?)`,
            [postSellId, post.ScheduleID, post.SellerID, buyerId, post.Price, getThaiTimeInMySQLFormat()]
        );
        const newTransactionId = result.insertId;

        // 5. เตรียมข้อความแจ้งเตือน (พ่วงคำเตือน Fatigue เข้าไปด้วย)
        const [shiftInfo] = await connection.query(
            "SELECT S.ShiftName FROM NurseSchedule NS JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE NS.ScheduleID = ?", 
            [post.ScheduleID]
        );
        const shiftName = shiftInfo[0]?.ShiftName || 'กะเวร';
        
        let notiMsg = `มีคนขอซื้อเวรวันที่ ${moment(post.Nurse_Date).format('DD/MM')} (${shiftName}) ของคุณ`;
        if (fatigueWarning) {
            notiMsg += `\n(ตรวจพบความเสี่ยง: ${safetyCheck.message})`;
        }

        // 6. บันทึก Notification ลงฐานข้อมูล
        await connection.query(
            `INSERT INTO Notifications (UserID, Title, Message, Type, RelatedDate, RelatedShift, RefID, CreatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                post.SellerID,
                '💰 มีคำขอซื้อเวรใหม่',
                notiMsg,
                'alert',
                post.Nurse_Date,
                shiftName,
                newTransactionId,
                getThaiTimeInMySQLFormat()
            ]
        );

        await connection.commit();
        
        // 7. ส่ง Socket แจ้งเตือนแบบ Real-time พร้อมส่ง Flag Fatigue ไปด้วย
        sendRealTimeNotification(post.SellerID, { 
            title: '💰 มีคำขอซื้อเวรใหม่', 
            message: notiMsg,
            isFatigue: !safetyCheck.safe,
            fatigueDetails: safetyCheck.message,
            type: 'alert' 
        });

        res.json({ 
            success: true, 
            message: "ส่งคำขอซื้อเรียบร้อยแล้ว",
            fatigueWarning: fatigueWarning // ส่งคำเตือนกลับไปให้ผู้ซื้อเห็นที่หน้าจอด้วย
        });

    } catch (err) {
        await connection.rollback();
        console.error("❌ Request Trade Error:", err.message);
        res.status(400).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

app.get('/api/market/my-requests/:userId', authenticateToken, async (req, res) => {
    const userId = req.params.userId;
    try {
        const sql = `SELECT ST.TransactionID, ST.Status, ST.CreatedAt as RequestDate, Seller.FirstName as OwnerName, NS.Nurse_Date, S.ShiftName FROM ShiftTransaction ST JOIN User Seller ON ST.SellerID = Seller.UserID JOIN NurseSchedule NS ON ST.ScheduleID = NS.ScheduleID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE ST.BuyerID = ? ORDER BY ST.TransactionID DESC`;
        const [results] = await dbPool.query(sql, [userId]);
        const formatted = results.map(row => ({ id: row.TransactionID, status: row.Status.toLowerCase(), created_at: row.RequestDate, title: `ขอซื้อเวร ${row.OwnerName}`, shift_date: row.Nurse_Date, note: row.ShiftName }));
        res.json(formatted);
    } catch (err) { console.error("My Status Error:", err); res.status(500).json({ success: false }); }
});

app.get('/api/market/my-active-posts/:userId', authenticateToken, async (req, res) => {
    const userId = req.params.userId;
    try {
        // ✅ เพิ่ม PS.ScheduleID ใน SELECT
        const sql = `SELECT PS.PostSellID, PS.ScheduleID, PS.Price, PS.Message, PS.CreatedAT, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, S.ShiftName FROM PostSell PS JOIN NurseSchedule NS ON PS.ScheduleID = NS.ScheduleID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE PS.UserID = ? AND PS.Status = 'Open' ORDER BY PS.CreatedAT DESC`;
        
        const [results] = await dbPool.query(sql, [userId]);
        const formatted = results.map(row => ({ 
            id: row.PostSellID, 
            schedule_id: row.ScheduleID, // ✅ สำคัญมาก! ต้องส่งค่านี้กลับไป
            price: row.Price, 
            message: row.Message, 
            shift_date: row.Nurse_Date, 
            shift_label: row.ShiftName, 
            created_at: row.CreatedAT 
        }));
        res.json({ success: true, posts: formatted });
    } catch (err) { res.status(500).json({ success: false, message: "DB Error" }); }
});

app.post('/api/market/delete-post', authenticateToken, async (req, res) => {
    const { postId } = req.body;
    try {
        const [check] = await dbPool.query("SELECT COUNT(*) as count FROM ShiftTransaction WHERE PostSellID = ?", [postId]);
        if (check[0].count > 0) return res.status(400).json({ success: false, message: "มีผู้กดขอซื้อรายการนี้อยู่ ลบไม่ได้" });
        await dbPool.query("DELETE FROM PostSell WHERE PostSellID = ?", [postId]);
        res.json({ success: true, message: "ลบประกาศเรียบร้อยแล้ว" });
    } catch (err) { console.error("Delete Error:", err); res.status(500).json({ success: false, message: "Server Error" }); }
});

app.post('/api/market/edit-post', authenticateToken, async (req, res) => {
    const { postId, price, message, scheduleId, userId } = req.body; // ✅ รับ scheduleId และ userId เพิ่ม
    
    if (!postId || !price || !scheduleId) return res.status(400).json({ success: false, message: "ข้อมูลไม่ครบ" });

    try {
        // 1. ตรวจสอบว่าโพสต์นี้มีคนกดซื้อไปหรือยัง (ถ้ามีห้ามแก้)
        const [check] = await dbPool.query("SELECT COUNT(*) as count FROM ShiftTransaction WHERE PostSellID = ? AND Status IN ('Pending_Seller', 'Pending_HeadNurse', 'Completed')", [postId]);
        if (check[0].count > 0) return res.status(400).json({ success: false, message: "แก้ไขไม่ได้: มีคนทำรายการซื้อขายค้างอยู่" });

        // 2. ถ้ามีการเปลี่ยนเวร (ScheduleID) ต้องเช็คว่าเวรใหม่ที่จะขาย ซ้ำกับประกาศอื่นหรือไม่?
        // (เช็ค PostSell อื่น ที่ไม่ใช่ PostID ปัจจุบัน)
        const [dupCheck] = await dbPool.query(
            "SELECT PostSellID FROM PostSell WHERE ScheduleID = ? AND Status = 'Open' AND PostSellID != ?", 
            [scheduleId, postId]
        );
        if (dupCheck.length > 0) {
            return res.status(400).json({ success: false, message: "เวรที่คุณเลือกใหม่ มีการลงประกาศขายไว้แล้ว" });
        }

        // 3. อัปเดตข้อมูล (รวมถึง ScheduleID)
        await dbPool.query(
            "UPDATE PostSell SET Price = ?, Message = ?, ScheduleID = ? WHERE PostSellID = ? AND UserID = ?", 
            [price, message, scheduleId, postId, userId]
        );

        res.json({ success: true, message: "แก้ไขข้อมูลสำเร็จ" });
    } catch (err) { 
        console.error("Edit Post Error:", err); 
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

app.get('/api/my-sellable-shifts/:userId', authenticateToken, async (req, res) => {
    const userId = req.params.userId;
    try {
        const sql = `SELECT NS.ScheduleID, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as Nurse_Date, S.ShiftName, S.StartTime, S.EndTime FROM NurseSchedule NS JOIN Shift S ON NS.Shift_id = S.Shift_id LEFT JOIN PostSell PS ON NS.ScheduleID = PS.ScheduleID AND PS.Status = 'Open' WHERE NS.UserID = ? AND NS.Nurse_Date >= CURRENT_DATE() AND PS.PostSellID IS NULL ORDER BY NS.Nurse_Date ASC`;
        const [results] = await dbPool.query(sql, [userId]);
        const shifts = results.map(row => ({ id: row.ScheduleID, label: `${row.Nurse_Date} | ${row.ShiftName} (${row.StartTime.slice(0,5)}-${row.EndTime.slice(0,5)})` }));
        res.json({ success: true, shifts });
    } catch (err) { console.error("Fetch Sellable Shifts Error:", err); res.status(500).json({ success: false, message: "Server Error" }); }
});

app.get('/api/market/incoming-requests/:sellerId', authenticateToken, async (req, res) => {
    const sellerId = req.params.sellerId;
    try {
        const sql = `SELECT ST.TransactionID, ST.Price, ST.Status, ST.CreatedAt, Buyer.FirstName, Buyer.LastName, Buyer.ProfileImage, S.ShiftName, DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as ShiftDate FROM ShiftTransaction ST JOIN User Buyer ON ST.BuyerID = Buyer.UserID JOIN NurseSchedule NS ON ST.ScheduleID = NS.ScheduleID JOIN Shift S ON NS.Shift_id = S.Shift_id WHERE ST.SellerID = ? AND ST.Status = 'Pending_Seller' ORDER BY ST.TransactionID DESC`;
        const [results] = await dbPool.query(sql, [sellerId]);
        res.json({ success: true, requests: results });
    } catch (err) { console.error("Incoming Request Error:", err); res.status(500).json({ success: false, message: "Server Error" }); }
});


// ✅ [ฉบับแก้ไข] API สำหรับผู้ขาย (Seller) ตอบรับหรือปฏิเสธคำขอซื้อเวร
app.post('/api/market/seller-respond', authenticateToken, async (req, res) => {
    const { transactionId, action } = req.body;
    const responderId = req.user.userId; 
    const connection = await dbPool.getConnection();

    try {
        await connection.beginTransaction();

        // 1. ตรวจสอบข้อมูลรายการซื้อขาย
        const [trans] = await connection.query(
            "SELECT * FROM ShiftTransaction WHERE TransactionID = ?", 
            [transactionId]
        );
        
        if (trans.length === 0) {
            throw new Error("ไม่พบรายการซื้อขายนี้ในระบบ");
        }
        const trade = trans[0];

        if (trade.SellerID != responderId) {
            throw new Error("คุณไม่มีสิทธิ์ดำเนินการในรายการนี้");
        }

        // 2. ดึงข้อมูลกะเวรเพื่อใช้แจ้งเตือน
        const [sched] = await connection.query(`
            SELECT DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as ShiftDate, S.ShiftName 
            FROM NurseSchedule NS 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            WHERE NS.ScheduleID = ?
        `, [trade.ScheduleID]);
        
        const shiftInfo = sched[0] || { ShiftDate: 'ไม่ระบุวันที่', ShiftName: 'ไม่ระบุกะ' };
        const nowTime = getThaiTimeInMySQLFormat();
        const createNotiSQL = `INSERT INTO Notifications 
            (UserID, Title, Message, Type, RelatedDate, RelatedShift, RefID, CreatedAt) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;

        if (action === 'reject') {
            // --- กรณีคนขายปฏิเสธ ---
            await connection.query(
                "UPDATE ShiftTransaction SET Status = 'Rejected' WHERE TransactionID = ?", 
                [transactionId]
            );

            // แจ้งเตือนคนซื้อ (Buyer)
            await connection.query(createNotiSQL, [
                trade.BuyerID, 
                '❌ ปฏิเสธการซื้อเวร', 
                `ผู้ขายปฏิเสธคำขอซื้อเวรวันที่ ${shiftInfo.ShiftDate} ของคุณ`, 
                'system', 
                shiftInfo.ShiftDate, 
                shiftInfo.ShiftName, 
                transactionId, 
                nowTime
            ]);

            sendRealTimeNotification(trade.BuyerID, { 
                title: 'คำขอซื้อเวรถูกปฏิเสธ', 
                message: 'ผู้ขายปฏิเสธคำขอของคุณ', 
                type: 'system' 
            });

        } else if (action === 'approve') {
            // --- กรณีคนขายอนุมัติ (ส่งต่อให้หัวหน้าพยาบาล) ---
            await connection.query(
                "UPDATE ShiftTransaction SET Status = 'Pending_HeadNurse' WHERE TransactionID = ?", 
                [transactionId]
            );

            // 🔥 [จุดที่เพิ่ม] แจ้งเตือนคนซื้อ (Buyer) ให้รู้ว่ารอหัวหน้าต่อ
            await connection.query(createNotiSQL, [
                trade.BuyerID, 
                '✅ ผู้ขายยืนยันแล้ว', 
                `รายการซื้อเวรวันที่ ${shiftInfo.ShiftDate} ได้รับการยืนยันจากผู้ขายแล้ว (รอหัวหน้าอนุมัติขั้นสุดท้าย)`, 
                'system', 
                shiftInfo.ShiftDate, 
                shiftInfo.ShiftName, 
                transactionId, 
                nowTime
            ]);

            // ส่ง Socket แจ้งคนซื้อให้หน้าจอเปลี่ยนสถานะ
            sendRealTimeNotification(trade.BuyerID, { 
                title: 'ผู้ขายยืนยันรายการแล้ว', 
                message: `เวรวันที่ ${shiftInfo.ShiftDate} กำลังรอหัวหน้าอนุมัติ`, 
                type: 'system' 
            });

            // 3. แจ้งเตือนหัวหน้าพยาบาลทุกคน (RoleID = 1)
            const [admins] = await connection.query("SELECT UserID FROM User WHERE RoleID = 1");
            
            for (const admin of admins) {
                await connection.query(createNotiSQL, [
                    admin.UserID, 
                    '⏳ รอตรวจสอบการซื้อขาย', 
                    'มีการตกลงซื้อขายเวรใหม่ รอการอนุมัติขั้นสุดท้ายจากท่าน', 
                    'alert', 
                    shiftInfo.ShiftDate, 
                    shiftInfo.ShiftName, 
                    transactionId, 
                    nowTime
                ]);
                
                sendRealTimeNotification(admin.UserID, { 
                    title: 'งานทะเบียน: รายการใหม่', 
                    message: 'มีรายการซื้อขายเวรผ่านการยืนยัน รอท่านอนุมัติ', 
                    type: 'alert' 
                });
            }
        }

        await connection.commit();
        res.json({ success: true, message: "ดำเนินการเรียบร้อยแล้ว" });

    } catch (err) {
        await connection.rollback();
        console.error("❌ Seller Respond Error:", err.message);
        res.status(500).json({ success: false, message: err.message });
    } finally {
        connection.release();
    }
});

app.get('/api/admin/market/pending', authenticateToken, async (req, res) => {
    // เช็คสิทธิ์ว่าเป็น Admin (Role 1)
    if (req.user.roleId !== 1) return res.status(403).json({ success: false });

    try {
        const sql = `
            SELECT 
                ST.TransactionID, 
                ST.Price, 
                ST.CreatedAt, 
                Seller.FirstName as SellerName, 
                Seller.LastName as SellerLast, 
                Buyer.FirstName as BuyerName, 
                Buyer.LastName as BuyerLast, 
                S.ShiftName, 
                DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') as ShiftDate 
            FROM ShiftTransaction ST 
            JOIN User Seller ON ST.SellerID = Seller.UserID 
            JOIN User Buyer ON ST.BuyerID = Buyer.UserID 
            JOIN NurseSchedule NS ON ST.ScheduleID = NS.ScheduleID 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            WHERE ST.Status = 'Pending_HeadNurse' 
            ORDER BY ST.CreatedAt DESC`;
            
        const [results] = await dbPool.query(sql);
        res.json({ success: true, results });
    } catch (err) {
        console.error("Fetch Pending Market Error:", err);
        res.status(500).json({ success: false, message: "DB Error" });
    }
});

app.post('/api/admin/market/action', authenticateToken, async (req, res) => {
    const { transactionId, action, adminId } = req.body;
    const connection = await dbPool.getConnection();
    
    console.log(`🚀 Processing Market Action: ID=${transactionId}, Action=${action}`);

    try {
        await connection.beginTransaction();

        // 1. ดึงข้อมูล Transaction และล็อคแถวไว้ป้องกันการทำงานซ้ำ
        const [trans] = await connection.query(`
            SELECT 
                ST.TransactionID, 
                ST.BuyerID, 
                ST.SellerID,
                ST.ScheduleID,
                ST.PostSellID,
                DATE_FORMAT(NS.Nurse_Date, '%Y-%m-%d') AS ShiftDate, 
                S.ShiftName 
            FROM ShiftTransaction ST 
            JOIN NurseSchedule NS ON ST.ScheduleID = NS.ScheduleID 
            JOIN Shift S ON NS.Shift_id = S.Shift_id 
            WHERE ST.TransactionID = ? 
            FOR UPDATE
        `, [transactionId]);

        if (trans.length === 0) {
            throw new Error("❌ ไม่พบรายการ Transaction ID นี้ในระบบ");
        }

        const trade = trans[0];

        const createNoti = `INSERT INTO Notifications (UserID, Title, Message, Type, RelatedDate, RelatedShift, RefID, CreatedAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
        const nowTime = getThaiTimeInMySQLFormat();
        
        let emailSubject = '';
        let emailMsgBuyer = '';
        let emailMsgSeller = '';

        if (action === 'reject') {
            // --- กรณีปฏิเสธ ---
            await connection.query("UPDATE ShiftTransaction SET Status = 'Rejected', ApprovedBy = ? WHERE TransactionID = ?", [adminId, transactionId]);
            
            const msgReject = 'หัวหน้าพยาบาลไม่อนุมัติคำขอซื้อเวรของคุณ เนื่องจากเหตุผลความปลอดภัยหรือความเหมาะสมของตารางวอร์ด';
            await connection.query(createNoti, [trade.BuyerID, '❌ การซื้อเวรถูกปฏิเสธ', msgReject, 'system', trade.ShiftDate, trade.ShiftName, transactionId, nowTime]);
            
            emailSubject = '❌ แจ้งเตือน: การซื้อเวรถูกปฏิเสธ';
            emailMsgBuyer = msgReject;

        } else if (action === 'approve') {
            // --- กรณีอนุมัติ ---
            await connection.query("UPDATE ShiftTransaction SET Status = 'Completed', ApprovedBy = ? WHERE TransactionID = ?", [adminId, transactionId]);
            
            // อัปเดตสถานะโพสต์ขาย
            if (trade.PostSellID) {
                await connection.query("UPDATE PostSell SET Status = 'Sold' WHERE PostSellID = ?", [trade.PostSellID]);
            }

            // ย้ายเจ้าของเวรในตารางหลัก
            await connection.query("UPDATE NurseSchedule SET UserID = ? WHERE ScheduleID = ?", [trade.BuyerID, trade.ScheduleID]);
            const msgSuccess = `หัวหน้าอนุมัติการซื้อขายเวรวันที่ ${moment(trade.ShiftDate).format('DD/MM/YYYY')} เรียบร้อยแล้ว ตารางเวรถูกอัปเดตแล้วครับ`;
            await connection.query(createNoti, [trade.BuyerID, '✅ การซื้อเวรสำเร็จ', msgSuccess, 'system', trade.ShiftDate, trade.ShiftName, transactionId, nowTime]);
            await connection.query(createNoti, [trade.SellerID, '✅ การขายเวรสำเร็จ', msgSuccess, 'system', trade.ShiftDate, trade.ShiftName, transactionId, nowTime]);

            emailSubject = '✅ แจ้งเตือน: รายการซื้อขายเวรสำเร็จ';
            emailMsgBuyer = msgSuccess;
            emailMsgSeller = msgSuccess;
        }

        await connection.commit();

        // 🚀 2. ส่งการแจ้งเตือนหลังจาก Commit สำเร็จแล้วเท่านั้น
        // ส่ง Socket Real-time
        sendRealTimeNotification(trade.BuyerID, { title: emailSubject, message: emailMsgBuyer, type: 'system' });
        if (action === 'approve') {
            sendRealTimeNotification(trade.SellerID, { title: emailSubject, message: emailMsgSeller, type: 'system' });
        }

        // ส่ง Email Push Notification
        await sendEmailNotification(trade.BuyerID, emailSubject, emailMsgBuyer);
        if (action === 'approve') {
            await sendEmailNotification(trade.SellerID, emailSubject, emailMsgSeller);
        }

        res.json({ success: true, message: "ดำเนินการเรียบร้อย" });

    } catch (err) {
        await connection.rollback();
        console.error("❌ Market Action Error:", err.message);
        res.status(500).json({ success: false, message: `เกิดข้อผิดพลาด: ${err.message}` });
    } finally {
        connection.release();
    }
});

// ปรับเป็น /api/posts/update/:postId เพื่อให้ตรงกับที่ Frontend ส่งมา
app.put('/api/posts/update/:postId', authenticateToken, async (req, res) => {
    try {
        // ดึง postId จาก URL หรือจาก Body ก็ได้ (กันเหนียว)
        const postId = req.params.postId || req.body.postId;
        const userId = req.user.userId; // ใช้ ID จาก Token เสมอเพื่อความปลอดภัย
        const { desiredDate, note, scheduleId } = req.body;

        if (!postId) return res.status(400).json({ success: false, message: 'ไม่พบรหัสโพสต์' });

        // 1. ตรวจสอบความเป็นเจ้าของและความมีอยู่ของโพสต์
        const [post] = await dbPool.query(
            "SELECT * FROM ExchangePost WHERE ExchangePostID = ? AND UserID = ?",
            [postId, userId]
        );

        if (post.length === 0) {
            return res.status(404).json({ success: false, message: 'ไม่พบประกาศ หรือคุณไม่มีสิทธิ์แก้ไขรายการนี้' });
        }

        // 2. อัปเดตข้อมูล
        const dateValue = (desiredDate && desiredDate !== "") ? desiredDate : null;
        const sql = `UPDATE ExchangePost SET DesiredShiftDate = ?, Message = ?, ScheduleID = ? WHERE ExchangePostID = ? AND UserID = ?`;
        
        await dbPool.query(sql, [dateValue, note, scheduleId, postId, userId]);

        res.json({ success: true, message: 'บันทึกการเปลี่ยนแปลงเรียบร้อย' });

    } catch (err) { 
        console.error("Update Post Error:", err); 
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

app.post('/api/admin/add-user', authenticateToken, async (req, res) => {
    try {
        if (req.user.roleId !== 1) return res.status(403).json({ success: false, message: 'Access Denied: Admins only' });
        const { email, firstName, lastName, roleId } = req.body;
        if (!email || !firstName) return res.status(400).json({ success: false, message: 'กรุณากรอก Email และชื่อจริง' });

        const rawPassword = generateRandomPassword(8);
        const hashedPassword = await bcrypt.hash(rawPassword, 10);

        // ✅ แก้ไข: ใช้เวลาจาก JS
        const sql = `INSERT INTO User (Email, PasswordHash, FirstName, LastName, RoleID, Status, MustChangePassword, CreatedAt) VALUES (?, ?, ?, ?, ?, 'active', 1, ?)`;
        
        await dbPool.query(sql, [email, hashedPassword, firstName, lastName || '', roleId || 2, getThaiTimeInMySQLFormat()]);

        const mailOptions = {
            from: '"AUTONURSESHIFT System" <ใช้อีเมลจริงที่ยืนยันกับ Brevo แล้ว>',
            to: email,
            subject: 'ยินดีต้อนรับเข้าสู่ระบบ - แจ้งรหัสผ่าน',
            html: `<div style="font-family: sans-serif; border: 1px solid #eee; padding: 20px; border-radius: 10px;">
                        <h2 style="color: #007bff;">ยินดีต้อนรับคุณ ${firstName}</h2>
                        <p>หัวหน้าพยาบาลได้เพิ่มคุณเข้าสู่ระบบ <b>AUTONURSESHIFT</b> เรียบร้อยแล้ว</p>
                        <p><b>ชื่อผู้ใช้ (Email):</b> ${email}</p>
                        <p><b>รหัสผ่านชั่วคราว:</b> <code style="background: #eee; padding: 4px 8px;">${rawPassword}</code></p>
                        <br>
                        <a href="{ลิงก์ Frontend}/login.html" style="background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">เข้าสู่ระบบที่นี่</a>
                        <p style="color: #d9534f; margin-top: 20px;">* คุณต้องเปลี่ยนรหัสผ่านทันทีหลังเข้าสู่ระบบครั้งแรก</p>
                    </div>`
        };
        transporter.sendMail(mailOptions).catch(err => console.error("Email Error:", err));
        res.json({ success: true, message: 'เพิ่มผู้ใช้งานเรียบร้อยแล้ว' });
    } catch (err) {
        if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ success: false, message: 'อีเมลนี้มีอยู่ในระบบแล้ว' });
        res.status(500).json({ success: false, message: 'Server Error: ' + err.message });
    }
});

app.post('/api/admin/generate-schedule', authenticateToken, async (req, res) => {
    console.time("GenerateScheduleTime"); // เริ่มจับเวลา
    if (req.user.roleId !== 1) return res.status(403).json({ success: false, message: "สิทธิ์ไม่ถูกต้อง" });

    const connection = await dbPool.getConnection();
    try {
        await connection.beginTransaction();
        const [nurses] = await connection.query("SELECT UserID, RoleID FROM User WHERE Status = 'active' AND RoleID IN (1, 2)");
        const targetMonth = moment().add(1, 'month');
        const targetMonthStr = targetMonth.startOf('month').format('YYYY-MM-DD');
        const [allConstraints] = await connection.query("SELECT * FROM Constraints WHERE SettingPeriod = ?", [targetMonthStr]);
        const targetYear = targetMonth.year();
        const yearMonth = targetMonth.format('YYYY-MM');
        const daysInMonth = targetMonth.daysInMonth();

        // 1. เตรียมข้อมูล Quota จาก Settings (ดึงแบบ JSON)
        const [settings] = await connection.query("SELECT SettingKey, SettingValue FROM SystemSettings");
        const parseQuota = (key) => {
            const setting = settings.find(s => s.SettingKey === key);
            if (!setting) return { 1: 0, 2: 0, 3: 0 };
            const val = JSON.parse(setting.SettingValue);
            return { 
                1: parseInt(val.morning || 0), 
                2: parseInt(val.afternoon || 0), 
                3: parseInt(val.night || 0) 
            };
        };

        const weekdayQuotas = parseQuota('WeekdayQuotas');
        const weekendQuotas = parseQuota('WeekendQuotas');
        const holidayQuotas = parseQuota('HolidayQuotas');


       

        // --- [เพิ่ม] ดึงข้อมูลวันหยุดออนไลน์สดๆ ---
        let holidaySet = new Set();
        try {
            const apiKey = process.env.CALENDARIFIC_API_KEY;
            const url = `https://calendarific.com/api/v2/holidays?&api_key=${apiKey}&country=TH&year=${targetYear}&type=national`;
            const hResponse = await axios.get(url);
            if (hResponse.data?.response?.holidays) {
                hResponse.data.response.holidays.forEach(h => holidaySet.add(h.date.iso));
            }
        } catch (apiErr) {
            console.error("⚠️ API Error:", apiErr.message);
        }

        // ข้อมูลตรวจสอบการต่อเวรข้ามเดือน
        const lastDayThisMonth = moment().endOf('month').format('YYYY-MM-DD');
        const [lastAssignments] = await connection.query("SELECT UserID, Shift_id FROM NurseSchedule WHERE Nurse_Date = ?", [lastDayThisMonth]);
        const lastShiftMap = new Map(lastAssignments.map(r => [r.UserID, r.Shift_id]));

        const constraintMap = new Map();
        allConstraints.forEach(c => {
            let rawData = c.FixedDaysOff;
            let parsedData = rawData ? (typeof rawData === 'string' ? JSON.parse(rawData.replace(/\\"/g, '"')) : rawData) : [];
            constraintMap.set(c.UserID, new Set(Array.isArray(parsedData) ? parsedData : []));
        });

        const workload = {}, monthlyNightWorkload = {}, monthlyAfternoonWorkload = {}, nurseHistory = {};
        nurses.forEach(n => {
            workload[n.UserID] = 0;
            monthlyNightWorkload[n.UserID] = 0;
            monthlyAfternoonWorkload[n.UserID] = 0;
            nurseHistory[n.UserID] = {
                consecutiveShifts: 0,
                lastShiftId: lastShiftMap.get(n.UserID) || null,
                weeklyShifts: 0,
                weeklyNightCount: 0,
                mustRest: false 
            };
        });
        const nightCount = {};
        const afternoonCount = {};
        nurses.forEach(n => {
            afternoonCount[n.UserID] = 0;
            nightCount[n.UserID] = 0;
        });

        let scheduleBuffer = [];
        let dailyAssignment = {};
        let incompleteShifts = [];

        // 2. ลูปจัดเวรรายวัน
        for (let d = 1; d <= daysInMonth; d++) {
            const currentDateStr = `${yearMonth}-${String(d).padStart(2, '0')}`;
            const currentDate = moment(currentDateStr);
            dailyAssignment[currentDateStr] = {};
            // --- [วางตรงนี้ครับ!] รีเซ็ตสถิติทุกเช้าวันจันทร์ ---
            if (currentDate.day() === 1) { 
                nurses.forEach(n => {
                    if (nurseHistory[n.UserID]) {
                        nurseHistory[n.UserID].weeklyShifts = 0;
                        nurseHistory[n.UserID].weeklyNightCount = 0; // ล้างค่าดึกรายสัปดาห์
                    }
                });
                console.log(`[Reset] Weekly stats cleared for Monday ${currentDateStr}`);
            }
            // ✅ [เพิ่ม] เลือก Quota ให้ตรงกับประเภทของวัน (ออนไลน์)
            let activeQuotas;
            if (holidaySet.has(currentDateStr)) activeQuotas = holidayQuotas;
            else if (currentDate.day() === 0 || currentDate.day() === 6) activeQuotas = weekendQuotas;
            else activeQuotas = weekdayQuotas;

            if (currentDate.day() === 1) nurses.forEach(n => nurseHistory[n.UserID].weeklyShifts = 0);

            // STEP A: หัวหน้า (เวรเช้า 1)
            const headNurses = nurses.filter(n => n.RoleID === 1);
            let headAssignedToday = false;

            for (const h of headNurses) {
                const hist = nurseHistory[h.UserID];
                
                // เงื่อนไขเดิม: ไม่ติดวันลา
                const isNotOnLeave = !constraintMap.get(h.UserID)?.has(currentDateStr);
                
                // --- เงื่อนไขใหม่: ตรวจสอบกฎการหยุดพัก ---
                const canWork = !hist.mustRest && hist.consecutiveShifts < 7;

                if (isNotOnLeave && canWork) {
                    scheduleBuffer.push([h.UserID, currentDateStr, 1]);
                    dailyAssignment[currentDateStr][h.UserID] = [1];
                    
                    // อัปเดตสถานะงาน
                    workload[h.UserID]++;
                    nurseHistory[h.UserID].consecutiveShifts++;
                    nurseHistory[h.UserID].weeklyShifts++;
                    nurseHistory[h.UserID].lastShiftId = 1;
                    
                    // ถ้าทำงานครบ 7 วันพอดี ให้เปิดใช้กฎบังคับพักในวันถัดไปทันที
                    if (nurseHistory[h.UserID].consecutiveShifts >= 7) {
                        nurseHistory[h.UserID].mustRest = true;
                    }
                    
                    headAssignedToday = true;
                    break; // เมื่อหัวหน้า 1 คนได้เวรแล้ว ให้จบการทำงาน STEP A
                }
            }

            // ==========================================
            // STEP B: จัดเวรพยาบาลทั่วไป (ลำดับ: บ่าย 2 -> ดึก 3 -> เช้า 1)
            // ==========================================
            [2, 3, 1].forEach(shiftId => {
                // เลือกโควตาที่ต้องใช้ในวันนั้นๆ (activeQuotas ดึงมาจาก JSON API ออนไลน์)
                let needed = (shiftId === 1 && headAssignedToday) ? activeQuotas[shiftId] - 1 : activeQuotas[shiftId];
                let gotCount = 0;

                // --- 1. ส่วนคัดเลือกคน (Filter) ---
                let candidates = nurses.filter(n => {
                    if (n.RoleID === 1) return false; 
                    const uid = n.UserID;
                    const hist = nurseHistory[uid];
                    const assignedToday = dailyAssignment[currentDateStr][uid] || [];

                    // กฎเหล็ก 7 วัน / วันลา / ควงไม่เกิน 2 / ไม่ซ้ำกะเดิม
                    if (hist.consecutiveShifts >= 7 || hist.mustRest) return false; 
                    if (constraintMap.get(uid)?.has(currentDateStr)) return false; 
                    if (assignedToday.length >= 2) return false;
                    if (assignedToday.includes(shiftId)) return false;

                    // กฎห้าม [ดึกวันนี้ -> เช้าพรุ่งนี้]
                    if (shiftId === 1 && hist.lastShiftId === 3) return false;

                    // กฎสำหรับเวรดึก (Shift 3)
                    if (shiftId === 3) {
                        if (assignedToday.includes(2)) return false; // ห้ามบ่ายต่อดึก
                        if (hist.weeklyNightCount >= 2) return false; // ✅ [กฎใหม่] ดึกห้ามเกิน 2 ต่อสัปดาห์
                        // ยอมให้ดึกมากกว่าบ่ายได้ไม่เกิน 1 เวร เพื่อให้ระบบไม่ "เดดล็อก"
                        if (d > 5 && nightCount[uid] > afternoonCount[uid] + 1) return false;
                        if (monthlyNightWorkload[uid] >= 10) return false; // เพดาน 10 ดึก/เดือน
                    }

                    if (hist.weeklyShifts >= 7) return false;
                    return true;
                });
                candidates.sort((a, b) => {
                    // 1. เช็ค Workload รวมก่อน (ใครงานน้อยต้องได้ทำก่อน)
                    const workloadDiff = (workload[a.UserID] || 0) - (workload[b.UserID] || 0);
                    
                    // ถ้า Workload ต่างกันมากกว่า 2 วัน ให้เลือกคนที่งานน้อยก่อนแน่นอน (เพื่อความบาลานซ์)
                    if (Math.abs(workloadDiff) > 2) {
                        return workloadDiff;
                    }

                    // 2. ถ้า Workload ใกล้เคียงกัน ให้เช็ค "หนี้เวร" เพื่อปลดล็อกกฎ บ่าย > ดึก
                    const debtA = nightCount[a.UserID] - afternoonCount[a.UserID];
                    const debtB = nightCount[b.UserID] - afternoonCount[b.UserID];
                    
                    return debtB - debtA; // ใครหนี้เวรเยอะกว่า (ดึกนำบ่าย) ให้แทรกคิวขึ้นมาเล็กน้อย
                });

                // --- 2. ส่วนบันทึกข้อมูล (Assign) ---
                for (let i = 0; i < needed && candidates.length > 0; i++) {
                    const candidate = candidates.shift();
                    const uid = candidate.UserID;
                    gotCount++;
                    
                    scheduleBuffer.push([uid, currentDateStr, shiftId]);
                    if (!dailyAssignment[currentDateStr][uid]) dailyAssignment[currentDateStr][uid] = [];
                    dailyAssignment[currentDateStr][uid].push(shiftId);
                    
                    // อัปเดตภาระงาน
                    workload[uid]++;
                    if (shiftId === 2) afternoonCount[uid]++; 
                    if (shiftId === 3) {
                        nightCount[uid]++;
                        monthlyNightWorkload[uid]++; 
                        // ✅ [ต้องใส่ตรงนี้!] บวกแต้มดึกรายสัปดาห์
                        nurseHistory[uid].weeklyNightCount++; 
                    }
                    
                    // อัปเดตประวัติการต่อเวร
                    nurseHistory[uid].consecutiveShifts++;
                    nurseHistory[uid].weeklyShifts++;
                    nurseHistory[uid].lastShiftId = shiftId;

                    // ถ้าครบ 7 เวรติด ให้เปิดสถานะต้องพัก
                    if (nurseHistory[uid].consecutiveShifts >= 7) {
                        nurseHistory[uid].mustRest = true;
                    }
                }
                // ✅ ต้องวาง console.log และ push ไว้ตรงนี้ (ก่อนปิดปีกกาของ forEach)
                console.log(`[DEBUG] วันที่: ${currentDateStr} | กะ: ${shiftId} | ต้องการ: ${needed} | จัดได้: ${gotCount}`);

               if (gotCount < needed) {
                    console.warn(`⚠️ เวรขาด! วันที่ ${currentDateStr} กะ ${shiftId} ขาดไป ${needed - gotCount} คน`);
                    
                    const shiftName = shiftId === 1 ? "เช้า" : (shiftId === 2 ? "บ่าย" : "ดึก");

                    incompleteShifts.push({ 
                        date: currentDateStr, 
                        shift: shiftName, 
                        // ถ้าเป็นกะเช้า (1) ให้บวก 1 (หัวหน้า) กลับเข้าไปทั้งตัวเลขที่ต้องการและที่จัดได้จริง
                        wanted: (shiftId === 1 && headAssignedToday) ? needed + 1 : needed, 
                        got: (shiftId === 1 && headAssignedToday) ? gotCount + 1 : gotCount 
                    });
                }
            });;
           
            // STEP C: อัปเดตสถานะการพักและรีเซ็ตประวัติสำหรับพยาบาลที่หยุดงาน
            nurses.forEach(n => {
                const uid = n.UserID;
                const isAssigned = dailyAssignment[currentDateStr][uid] && dailyAssignment[currentDateStr][uid].length > 0;

                if (!isAssigned) {
                    // กรณีพยาบาล "ไม่ได้ลงเวร" ในวันนี้ (ถือว่าเป็นวันหยุด)
                    
                    // 1. ถ้าเขาทำครบ 7 เวร แล้วต้องถูกบังคับพัก (mustRest เป็น true)
                    // เราจะให้เขาพักไปเรื่อยๆ จนกว่า willReset จะเป็น true (ในที่นี้คือเมื่อเขาหยุดครบ 1 วัน)
                    if (nurseHistory[uid].mustRest) {
                        nurseHistory[uid].consecutiveShifts = 0;
                        nurseHistory[uid].mustRest = false; // ปลดล็อกสถานะการบังคับพัก
                        nurseHistory[uid].lastShiftId = null;
                    } else {
                        // กรณีหยุดพักปกติ (ไม่ใช่การหยุดหลังจากครบ 7 เวร)
                        // ให้รีเซ็ตประวัติการต่อเวรเพื่อไม่ให้กฎการต่อเวรค้างข้ามวัน
                        nurseHistory[uid].consecutiveShifts = 0;
                        nurseHistory[uid].lastShiftId = null;
                    }
                } else {
                    // กรณี "มีการลงเวร" ในวันนี้
                    // ตรวจสอบว่าพยาบาลคนนี้ถึงขีดจำกัด 7 เวรหรือไม่
                    if (nurseHistory[uid].consecutiveShifts >= 7) {
                        nurseHistory[uid].mustRest = true;
                    }
                }
            });
        }

        // 3. บันทึกเข้า DB
        if (scheduleBuffer.length > 0) {
            await connection.query("DELETE FROM NurseSchedule WHERE Nurse_Date LIKE ?", [`${yearMonth}%`]);
            await connection.query("INSERT INTO NurseSchedule (UserID, Nurse_Date, Shift_id) VALUES ?", [scheduleBuffer]);
        }

        await connection.commit();
        console.timeEnd("GenerateScheduleTime"); // จบจับเวลา
        res.json({ 
                success: true, 
                message: "จัดเวรสำเร็จ", 
                incompleteShifts: incompleteShifts 
            });

        } catch (err) {
            if (connection) await connection.rollback();
            res.status(500).json({ success: false, message: "Error: " + err.message });
        } finally {
            if (connection) connection.release();
        }
    });
// Add this route to server.js

app.get('/api/admin/team-stats', authenticateToken, async (req, res) => {
    const { month, year } = req.query;

    if (!month || !year) {
        return res.status(400).json({ success: false, message: "กรุณาระบุเดือนและปี" });
    }

    try {

        // สร้างช่วงวันที่ (ใช้ index ได้)
        const startDate = `${year}-${month}-01`;
        const endDate = moment(startDate).add(1,'month').format('YYYY-MM-DD');

        // 1. Ward Summary
        const sqlWardStats = `
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN Shift_id = 1 THEN 1 ELSE 0 END) as morning,
                SUM(CASE WHEN Shift_id = 2 THEN 1 ELSE 0 END) as afternoon,
                SUM(CASE WHEN Shift_id = 3 THEN 1 ELSE 0 END) as night
            FROM NurseSchedule 
            WHERE Nurse_Date >= ? AND Nurse_Date < ?
        `;

        // 2. Individual Nurse Stats
        const sqlNursesStats = `
            SELECT 
                U.UserID, 
                U.FirstName, 
                U.LastName, 
                U.ProfileImage,
                COUNT(NS.ScheduleID) as total,
                SUM(CASE WHEN NS.Shift_id = 1 THEN 1 ELSE 0 END) as m,
                SUM(CASE WHEN NS.Shift_id = 2 THEN 1 ELSE 0 END) as a,
                SUM(CASE WHEN NS.Shift_id = 3 THEN 1 ELSE 0 END) as n
            FROM User U
            LEFT JOIN NurseSchedule NS 
                ON U.UserID = NS.UserID 
                AND NS.Nurse_Date >= ? 
                AND NS.Nurse_Date < ?
            WHERE U.RoleID IN (1,2) 
            AND U.Status = 'active'
            GROUP BY U.UserID
            ORDER BY total DESC, U.FirstName ASC
        `;

        // 3. Daily Details
        const sqlDailyDetails = `
            SELECT 
                NS.UserID,
                DATE_FORMAT(NS.Nurse_Date,'%d') as d,
                DATE_FORMAT(NS.Nurse_Date,'%W') as dayName,
                S.ShiftName as type
            FROM NurseSchedule NS
            JOIN Shift S ON NS.Shift_id = S.Shift_id
            WHERE NS.Nurse_Date >= ? AND NS.Nurse_Date < ?
            ORDER BY NS.Nurse_Date ASC
        `;

        // Query พร้อมกัน
        const [
            [wardStats],
            [nursesRes],
            [dailyRes]
        ] = await Promise.all([
            dbPool.query(sqlWardStats,[startDate,endDate]),
            dbPool.query(sqlNursesStats,[startDate,endDate]),
            dbPool.query(sqlDailyDetails,[startDate,endDate])
        ]);

        // สร้าง Map สำหรับ lookup เร็ว
        const scheduleMap = {};

        dailyRes.forEach(row => {
            if (!scheduleMap[row.UserID]) {
                scheduleMap[row.UserID] = [];
            }

            scheduleMap[row.UserID].push({
                d: row.d,
                day: row.dayName,
                type: row.type
            });
        });

        // Process Nurse Data
        const processedNurses = nursesRes.map(nurse => {

            let status = 'ปกติ';

            if (nurse.n > 8) status = 'ดึกหนัก';
            else if (nurse.total > 24) status = 'งานโหลด';
            else if (nurse.total === 0) status = 'ไม่มีเวร';

            return {
                id: nurse.UserID,
                name: `${nurse.FirstName} ${nurse.LastName}`,
                img: nurse.ProfileImage,
                total: nurse.total || 0,
                m: nurse.m || 0,
                a: nurse.a || 0,
                n: nurse.n || 0,
                status: status,
                dates: scheduleMap[nurse.UserID] || []
            };
        });

        // Thai month
        const monthNameThai = moment(`${year}-${month}-01`)
            .locale('th')
            .format('MMMM') + ' ' + (parseInt(year) + 543);

        res.json({
            success: true,
            monthName: monthNameThai,
            wardTotal: wardStats[0].total || 0,
            totalM: wardStats[0].morning || 0,
            totalA: wardStats[0].afternoon || 0,
            totalN: wardStats[0].night || 0,
            nurses: processedNurses
        });

    } catch (err) {
        console.error("Team Stats Error:", err);
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดทางเทคนิค (Database Error)" });
    }
});
// ==========================================
// ส่วนที่ขาดหายไป: API เช็คสถานะการส่งเวร
// ==========================================
app.get('/api/constraint-status', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.userId;
        // คำนวณเดือนหน้า (ที่เป็นรอบส่งเวร)
        const nextMonthStr = moment().add(1, 'month').startOf('month').format('YYYY-MM-DD');
        
        // 1. นับจำนวนพยาบาลทั้งหมดที่มีสิทธิ์ (Role 1, 2)
        const [totalRes] = await dbPool.query("SELECT COUNT(*) as count FROM User WHERE RoleID IN (1, 2) AND Status = 'active'");
        
        // 2. นับคนที่ส่งข้อมูลมาแล้วในรอบเดือนหน้า
        const [submittedRes] = await dbPool.query("SELECT COUNT(DISTINCT UserID) as count FROM Constraints WHERE SettingPeriod = ?", [nextMonthStr]);
        
        // 3. เช็คว่า "ตัวเราเอง" ส่งหรือยัง
        const [myRes] = await dbPool.query("SELECT COUNT(*) as count FROM Constraints WHERE UserID = ? AND SettingPeriod = ?", [userId, nextMonthStr]);
        
        res.json({ 
            success: true, 
            total: totalRes[0].count, 
            submitted: submittedRes[0].count, 
            myStatus: myRes[0].count > 0 
        });
    } catch (err) { 
        console.error("Constraint Status Error:", err); 
        res.status(500).json({ success: false, message: "Server Error" }); 
    }
});

// ✅ API สำหรับ Admin ดูรายชื่อผู้ใช้ทั้งหมด
app.get('/api/admin/all-users', authenticateToken, async (req, res) => {
    try {
        // (Optional) เช็คว่าเป็น Admin จริงหรือไม่? (RoleID = 1 คือ Admin/Head Nurse)
        if (req.user.roleId !== 1) {
            return res.status(403).json({ success: false, message: 'Access Denied: Admin Only' });
        }

        const [users] = await dbPool.query(`
            SELECT 
                UserID, 
                FirstName, 
                LastName, 
                Email, 
                RoleID,    
                ProfileImage, 
                Status 
            FROM User 
            ORDER BY RoleID ASC, UserID ASC
        `)

        res.json({ success: true, users: users });

    } catch (err) {
        console.error("Fetch Users Error:", err);
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการดึงข้อมูลผู้ใช้" });
    }
});
// ✅ API สำหรับอัปเดตสถานะผู้ใช้งาน (ฉบับปรับปรุงให้ตรงกับหน้าบ้าน)
app.post('/api/admin/update-user-status', authenticateToken, async (req, res) => {
    const { targetUserId, newStatus } = req.body;
    const currentAdminId = req.user.userId; // ID ของหัวหน้าพยาบาลที่กำลังใช้งานระบบ

    try {
        // กฎความปลอดภัย: ห้ามหัวหน้าพยาบาลปิดสถานะ (Inactive) ตัวเอง
        // เพราะจะทำให้ไม่มีใครล็อกอินเข้ามาเปิดคืนได้หากในระบบมีหัวหน้าคนเดียว
        if (parseInt(targetUserId) === parseInt(currentAdminId) && newStatus === 'inactive') {
            return res.json({ 
                success: false, 
                message: "ไม่สามารถปิดสถานะตัวเองได้ โปรดให้หัวหน้าพยาบาลท่านอื่นเป็นผู้ดำเนินการแทน" 
            });
        }

        const [result] = await dbPool.query(
            "UPDATE User SET Status = ? WHERE UserID = ?",
            [newStatus, targetUserId]
        );

        if (result.affectedRows > 0) {
            res.json({ success: true, message: "อัปเดตสถานะบุคลากรเรียบร้อยแล้ว" });
        } else {
            res.json({ success: false, message: "ไม่พบข้อมูลพยาบาลที่ระบุ" });
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดที่เซิร์ฟเวอร์" });
    }
});

// 1. ดึงข้อมูลพยาบาลที่ถูกจัดเวรแล้วในวันที่เลือก
app.get('/api/admin/daily-schedule', authenticateToken, async (req, res) => {
    const { date } = req.query; // รับค่าวันที่ เช่น 2026-02-01
    try {
        const sql = `
            SELECT NS.ScheduleID, NS.UserID, U.FirstName, U.LastName, NS.Shift_id 
            FROM NurseSchedule NS 
            JOIN User U ON NS.UserID = U.UserID 
            WHERE NS.Nurse_Date = ?
            ORDER BY NS.Shift_id ASC`;
        const [shifts] = await dbPool.query(sql, [date]);
        res.json({ success: true, shifts });
    } catch (err) {
        res.status(500).json({ success: false, message: "Database Error" });
    }
});

// 2. ดึงรายชื่อพยาบาล "ทั้งหมด" ที่ Active เพื่อใช้ใน Dropdown
app.get('/api/admin/all-nurses', authenticateToken, async (req, res) => {
    try {
        // ดึงเฉพาะพยาบาล (RoleID 2) และหัวหน้าพยาบาล (RoleID 1) ที่สถานะปกติ
        const [nurses] = await dbPool.query(
            "SELECT UserID, FirstName, LastName FROM User WHERE RoleID IN (1, 2) AND Status = 'active' ORDER BY FirstName ASC"
        );
        res.json({ success: true, nurses });
    } catch (err) {
        res.status(500).json({ success: false, message: "Database Error" });
    }
});

// 3. เพิ่มเวรด้วยมือ (พร้อมระบบตรวจสอบกฎเบื้องต้น)
app.post('/api/admin/add-shift-manual', authenticateToken, async (req, res) => {
    const { userId, date, shiftId } = req.body;

    try {
        // --- กฎเหล็กที่ 1: ห้ามลงกะซ้ำเดิมในวันเดียวกัน ---
        const [dupShift] = await dbPool.query(
            "SELECT * FROM NurseSchedule WHERE UserID = ? AND Nurse_Date = ? AND Shift_id = ?",
            [userId, date, shiftId]
        );
        if (dupShift.length > 0) return res.json({ success: false, message: "พยาบาลคนนี้อยู่ในกะนี้แล้ว" });

        // --- กฎเหล็กที่ 2: หนึ่งวันห้ามเกิน 2 กะ ---
        const [dayLimit] = await dbPool.query(
            "SELECT COUNT(*) as count FROM NurseSchedule WHERE UserID = ? AND Nurse_Date = ?",
            [userId, date]
        );
        if (dayLimit[0].count >= 2) return res.json({ success: false, message: "พยาบาลคนนี้ลงเวรเต็มโควตาต่อวันแล้ว (2 กะ)" });

        // บันทึกข้อมูล
        await dbPool.query(
            "INSERT INTO NurseSchedule (UserID, Nurse_Date, Shift_id) VALUES (?, ?, ?)",
            [userId, date, shiftId]
        );
        res.json({ success: true, message: "เพิ่มพยาบาลเข้ากะสำเร็จ" });

    } catch (err) {
        res.status(500).json({ success: false, message: "Database Error: " + err.message });
    }
});

// 4. ลบรายชื่อพยาบาลออกจากเวร
app.delete('/api/admin/delete-shift/:id', authenticateToken, async (req, res) => {
    const scheduleId = req.params.id;
    try {
        await dbPool.query("DELETE FROM NurseSchedule WHERE ScheduleID = ?", [scheduleId]);
        res.json({ success: true, message: "ลบรายการเวรสำเร็จ" });
    } catch (err) {
        res.status(500).json({ success: false, message: "Database Error" });
    }
});
app.get('/api/get-my-constraints', authenticateToken, async (req, res) => {
    const userId = req.user.userId; 
    const { period } = req.query;

    if (!period) {
        return res.status(400).json({ success: false, message: "กรุณาระบุรอบเดือน" });
    }

    try {
        const sql = `SELECT * FROM Constraints WHERE UserID = ? AND SettingPeriod = ? LIMIT 1`;
        const [rows] = await dbPool.query(sql, [userId, period]);

        if (rows.length > 0) {
            res.json({ success: true, data: rows[0] });
        } else {
            // กรณีไม่มีข้อมูลเดิม ส่ง success false เพื่อให้หน้าบ้านไม่ต้องเติมฟอร์ม
            res.json({ success: false, message: "ยังไม่มีข้อมูลเดิม" });
        }
    } catch (err) {
        console.error("Fetch Constraints Error:", err);
        res.status(500).json({ success: false, message: "Database Error" });
    }
});

// ==========================================
// 10. SMART NOTIFICATION ENGINE (ฉบับส่ง Email Reminder)
// ==========================================
async function runSmartNotificationLogic() {
    console.log(`[${new Date().toISOString()}] 🤖 Smart Notification Engine: Starting...`);
    const connection = await dbPool.getConnection();
    
    try {
        const tomorrow = moment().add(1, 'days').format('YYYY-MM-DD');
        
        // 1. ดึงรายชื่อพยาบาลที่เตือนไปแล้ววันนี้ (ดึงครั้งเดียว)
        const [alreadyNotified] = await connection.query(
            "SELECT UserID FROM Notifications WHERE Type = 'reminder' AND RelatedDate = ?", 
            [tomorrow]
        );
        const notifiedSet = new Set(alreadyNotified.map(n => n.UserID));

        // 2. ดึงตารางเวรพรุ่งนี้ เฉพาะคนที่มีสถานะ active
        const [shifts] = await connection.query(
            `SELECT NS.UserID, S.ShiftName, S.StartTime, U.FirstName, U.Email 
             FROM NurseSchedule NS 
             JOIN Shift S ON NS.Shift_id = S.Shift_id 
             JOIN User U ON NS.UserID = U.UserID
             WHERE NS.Nurse_Date = ? AND U.Status = 'active'`, 
            [tomorrow]
        );

        for (const shift of shifts) {
            // ข้ามถ้าเคยส่งไปแล้ว (เช็คจาก Memory)
            if (notifiedSet.has(shift.UserID)) continue;

            // ครอบ try-catch เพื่อไม่ให้ Error ของคนเดียวทำระบบพังทั้งหมด
            try {
                const msg = `อย่าลืม! พรุ่งนี้คุณมีเวร ${shift.ShiftName} เวลา ${shift.StartTime.slice(0,5)} น.`;

                // บันทึก Log ลง DB (ใช้ INSERT IGNORE กันเหนียว)
                await connection.query(
                    `INSERT IGNORE INTO Notifications (UserID, Title, Message, Type, RelatedDate, RelatedShift, CreatedAt) 
                     VALUES (?, '⏰ เตือนตารางเวร', ?, 'reminder', ?, ?, ?)`, 
                    [shift.UserID, msg, tomorrow, shift.ShiftName, getThaiTimeInMySQLFormat()]
                );

                // ส่ง Socket สำหรับคนที่เปิดหน้าเว็บอยู่
                sendRealTimeNotification(shift.UserID, {
                    title: '⏰ เตือนตารางเวร',
                    message: msg,
                    type: 'reminder'
                });

                // ส่ง Email
                const emailSubject = `⏰ แจ้งเตือน: คุณมีเวรในวันพรุ่งนี้ (${moment(tomorrow).format('DD/MM/YYYY')})`;
                const emailHtml = `<div style="text-align: center; padding: 10px;">
                        <img src="https://res.cloudinary.com/your-cloud-name/image/upload/v1/assets/calendar-icon.png" alt="Reminder" style="width: 80px; margin-bottom: 20px;"> // ให้น้องอัปโหลดรูป Icon ปฏิทินขึ้น Cloudinary ของน้องเอง แล้วเอาลิงก์มาเปลี่ยนตรงนี้
                        <p style="font-size: 18px; color: #333;">แจ้งเตือนการเข้าเวรสำหรับวันพรุ่งนี้</p>
                        <div style="background-color: #f8fbff; border: 1px solid #007bff; border-radius: 10px; padding: 20px; margin: 20px 0;">
                            <h3 style="color: #007bff; margin: 0;">${shift.ShiftName}</h3>
                            <p style="font-size: 24px; font-weight: bold; margin: 10px 0;">เวลา ${shift.StartTime.slice(0,5)} น.</p>
                            <p style="margin: 0; color: #666;">วันที่: ${moment(tomorrow).format('LL')}</p>
                        </div>
                        <p style="color: #d9534f;">* กรุณาเตรียมตัวและเข้าเวรให้ตรงเวลา</p>
                    </div>
                `;;
                
                // ส่งเมลและดักจับ Error รายคน
                await sendEmailNotification(shift.UserID, emailSubject, emailHtml);
                
                console.log(`📧 Reminder sent to: ${shift.Email}`);

            } catch (innerErr) {
                console.error(`❌ ข้ามการส่งของ UserID ${shift.UserID} เนื่องจาก:`, innerErr.message);
            }
        }
        
    } catch (err) {
        console.error("❌ Smart Notification Global Error:", err);
    } finally {
        connection.release(); 
    }
}


cron.schedule('0 20 * * *', () => {
    console.log(`⏰ Cron Job: Running Notification Engine at ${getThaiTimeInMySQLFormat()}`);
    runSmartNotificationLogic();
});

// ==========================================
// 11. SYSTEM CLEANUP ENGINE
// ==========================================
async function runCleanupTask() {
    console.log(`[${new Date().toISOString()}] 🧹 Cleanup Engine: Starting...`);
    const connection = await dbPool.getConnection();
    
    try {
        // เก็บจำนวนที่ลบได้รวมกัน
        let totalDeleted = 0;

        const [otpRes] = await connection.query(`DELETE FROM Password_reset_otp WHERE created_at < DATE_SUB(NOW(), INTERVAL 1 DAY)`);
        totalDeleted += otpRes.affectedRows;

        const [logRes] = await connection.query(`DELETE FROM LoginLog WHERE CreatedAt < DATE_SUB(NOW(), INTERVAL 3 MONTH)`);
        totalDeleted += logRes.affectedRows;

        const [notiRes] = await connection.query(`DELETE FROM Notifications WHERE CreatedAt < DATE_SUB(NOW(), INTERVAL 6 MONTH)`);
        totalDeleted += notiRes.affectedRows;

        console.log(`[${new Date().toISOString()}] ✅ Cleanup Engine: Finished. Total deleted: ${totalDeleted}`);
        
        return { affectedRows: totalDeleted }; // ✅ ส่งค่ากลับไปให้ API

    } catch (err) {
        console.error("❌ Cleanup Error:", err);
        throw err; // โยน error ต่อไปที่ API catch
    } finally {
        connection.release();
    }
}

// ==========================================
// 12. EXTERNAL CRON TRIGGER ROUTE (ฉบับแก้ไขเพื่อลด Output)
// ==========================================
app.get('/api/cron/trigger-notifications', async (req, res) => {
    try {
        // 1. ตรวจสอบ Secret Key
        if (req.query.key !== process.env.CRON_SECRET) {
            return res.status(401).json({ success: false }); // ส่งแค่นี้พอ ไม่ต้องส่ง message ยาว
        }

        const now = moment().utcOffset(7);
        const currentHour = now.hour();
        const todayStr = now.format('YYYY-MM-DD');

        // 2. ถ้าไม่ใช่ช่วงเวลา 20.00 น. (กรณี cron-job เรียกมาเพื่อกันหลับ)
        if (currentHour !== 20) {
            // ส่ง Response สั้นที่สุด เพื่อลดขนาด Output
            return res.json({ s: "ping_ok" }); 
        }

        // 3. ถ้าเป็นช่วง 20.00 น. เช็คว่าส่งไปหรือยัง
        const [alreadySent] = await dbPool.query(
            "SELECT NotiID FROM Notifications WHERE Type = 'reminder' AND DATE(CreatedAt) = ? LIMIT 1",
            [todayStr]
        );

        if (alreadySent.length > 0) {
            return res.json({ s: "already_done" });
        }

        // 4. รัน Logic แจ้งเตือน
        // ใช้ await เพื่อให้ทำงานเสร็จก่อนตอบกลับ แต่อย่าส่งข้อมูล User กลับไปใน res.json
        await runSmartNotificationLogic();
        
        // ตอบกลับสั้นๆ ว่าสำเร็จ
        res.json({ s: "executed", d: todayStr });

    } catch (err) {
        console.error("Cron Route Error:", err);
        // ถึงจะ Error ก็ส่งสั้นๆ เพื่อไม่ให้ cron-job.org เก็บ Log บวม
        res.status(500).json({ s: "error" });
    }
});

async function runBackupTask() {
    console.log(`[${new Date().toISOString()}] 📦 Backup Engine: Starting...`);
    
    const dateStr = moment().utcOffset(7).format('YYYY-MM-DD');
    const sqlFileName = `backup-${dateStr}.sql`;
    const zipFileName = `${sqlFileName}.gz`; // บีบอัดเพื่อประหยัดที่ GitHub
    const filePath = path.join('/tmp', sqlFileName); 
    const backupDir = '/tmp/git_backup'; 

    const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

    try {
        // 1. Dump ข้อมูลจาก TiDB
        await mysqldump({
            connection: {
                host: process.env.DB_HOST,
                user: process.env.DB_USER,
                password: process.env.DB_PASSWORD,
                database: process.env.DB_DATABASE,
                port: process.env.DB_PORT || 4000,
                ssl: { minVersion: 'TLSv1.2', rejectUnauthorized: true }
            },
            dumpToFile: filePath,
        });

        console.log(`✅ SQL Dump Created: ${sqlFileName}`);

        // 2. ใช้ Git Clone เพื่อดึงไฟล์วันเก่าๆ ลงมา (เพื่อให้ไฟล์ไม่หาย)
        const gitCommands = `
            rm -rf ${backupDir} && 
           // ✅ สิ่งที่น้องต้องเปลี่ยนชื่อ github ที่ใช้ blackup
            git clone https://${GITHUB_TOKEN}@github.com/your-username/your-repo-name.git ${backupDir} && 
            gzip -c ${filePath} > ${backupDir}/${zipFileName} && 
            cd ${backupDir} && 
            git config user.name "Auto Backup" && 
            git config user.email "อีเมล GitHub ของน้องเอง" && 
            git add . && 
            git commit -m "Auto-backup: ${dateStr}" && 
            git push origin main
        `;

        exec(gitCommands, (error, stdout, stderr) => {
            if (error) {
                console.error(`❌ Git Error: ${error.message}`);
            } else {
                console.log(`🚀 Backup Pushed Successfully! (File: ${zipFileName})`);
            }

            // 3. Cleanup: ลบทุกอย่างใน /tmp
            if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
            exec(`rm -rf ${backupDir}`);
            console.log(`Sweep! 🧹 Temporary files cleaned.`);
        });

    } catch (err) {
        console.error("❌ Backup Failure:", err);
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
        exec(`rm -rf ${backupDir}`);
    }
}
app.get('/api/cron/cleanup', async (req, res) => {
    // ป้องกันการแอบเรียกโดยไม่มี Key
    if (!req.query.key || req.query.key !== process.env.CRON_SECRET) {
        return res.status(401).send("u"); // ส่งแค่ตัวเดียวพอ
    }

    try {
        const result = await runCleanupTask(); 
        
        // ส่ง Response สั้นๆ เพื่อไม่ให้ Buffer ของ Render เต็ม
        res.status(200).json({ 
            s: "ok", 
            c: result?.affectedRows || 0 
        });
    } catch (err) {
        console.error("❌ API Cleanup Error:", err.message);
        res.status(500).json({ s: "err" });
    }
});
// 2. ปรับตัวรับงานจากภายนอก (External Trigger)
app.get('/api/cron/trigger-backup', async (req, res) => {
    try {
        if (req.query.key !== process.env.CRON_SECRET) {
            return res.status(401).send("unauthorized");
        }

        const now = moment().utcOffset(7);
        // ตรวจสอบว่าอยู่ในช่วงตี 3 ถึง ตี 4 หรือไม่ (เพื่อป้องกันการแอบรันเวลาอื่น)
        if (now.hour() !== 3 && now.hour() !== 4) {
            return res.json({ s: "not_the_time" });
        }

        // เริ่มทำงาน Backup ทันที
        console.log("🚀 External Trigger: Starting Backup Task...");
        runBackupTask(); // ไม่ต้องใส่ await ก็ได้ถ้าไม่อยากให้ Request รอนานเกินไป

        res.json({ s: "backup_started", t: now.format('HH:mm:ss') });
    } catch (err) {
        res.status(500).json({ s: "error" });
    }
});
app.get("/api/admin/fatigue-check", authenticateToken, async (req, res) => {
    const { userId, date, shiftId } = req.query;

    if (!userId || !date || !shiftId) {
        return res.status(400).json({ success: false, message: "ข้อมูลไม่ครบถ้วน" });
    }

    try {
        // เรียกใช้ฟังก์ชันมาตรฐานข้อที่ระบบใช้อยู่ (กฎ 6 ข้อที่สรุปไว้)
        const result = await checkFatigueStatus(dbPool, userId, date, shiftId);

        // ส่งผลลัพธ์กลับในรูปแบบที่หน้าบ้านเข้าใจ
        res.json({
            success: true,
            safe: result.safe,
            isWarning: result.isWarning || false,
            riskLevel: result.safe ? (result.isWarning ? "MODERATE" : "LOW") : "HIGH",
            message: result.message
        });

    } catch (err) {
        console.error("Fatigue Check API Error:", err);
        res.status(500).json({ success: false, message: "เกิดข้อผิดพลาดในการตรวจสอบความล้า" });
    }
});
app.get("/api/public-holidays", async (req, res) => {
    try {
        const year = req.query.year || 2026;
        const month = req.query.month;
        const apiKey = process.env.CALENDARIFIC_API_KEY; 

        const holidayThaiNames = {
            // --- มกราคม ---
            "New Year's Day": "วันขึ้นปีใหม่",
            "New Year Special Holiday": "วันหยุดพิเศษปีใหม่",

            // --- กุมภาพันธ์/มีนาคม (วันสำคัญทางศาสนา) ---
            "Makha Bucha": "วันมาฆบูชา",
            "Makha Bucha Day": "วันมาฆบูชา",
            "Makha Bucha Day (Observed)": "ชดเชยวันมาฆบูชา",

            // --- เมษายน ---
            "Chakri Day": "วันจักรี",
            "Chakri Memorial Day": "วันจักรี",
            "Songkran": "วันสงกรานต์",
            "Songkran Day": "วันสงกรานต์",
            "Songkran Festival": "วันสงกรานต์",
            "Songkran Holiday": "วันหยุดสงกรานต์",

            // --- พฤษภาคม ---
            "Labor Day": "วันแรงงาน",
            "Labor Day Observed": "ชดเชยวันแรงงาน",
            "Coronation Day": "วันฉัตรมงคล",
            "Visakha Bucha": "วันวิสาขบูชา",
            "Visakha Bucha Day": "วันวิสาขบูชา",
            "Day off for Visakha Bucha": "ชดเชยวันวิสาขบูชา",

            // --- มิถุนายน ---
            "Queen Suthida's Birthday": "วันเฉลิมฯ พระราชินี",

            // --- กรกฎาคม ---
            "Asalha Bucha": "วันอาสาฬหบูชา",
            "Asarnha Bucha Day": "วันอาสาฬหบูชา",
            "Khao Phansa Day": "วันเข้าพรรษา",
            "King Vajiralongkorn's Birthday": "วันเฉลิมฯ ร.10",
            "H.M. King Maha Vajiralongkorn's Birthday": "วันเฉลิมฯ ร.10",

            // --- สิงหาคม ---
            "The Queen Mother's Birthday": "วันแม่แห่งชาติ",
            "The Queen's Birthday": "วันแม่แห่งชาติ",
            "H.M. Queen Sirikit's Birthday": "วันแม่แห่งชาติ",

            // --- ตุลาคม ---
            "Anniversary of the Death of King Bhumibol": "วันคล้ายวันสวรรคต ร.9",
            "H.M. King Bhumibol Adulyadej Memorial Day": "วันคล้ายวันสวรรคต ร.9",
            "Chulalongkorn Day": "วันปิยมหาราช",
            "Day off for Chulalongkorn Day": "ชดเชยวันปิยมหาราช",

            // --- ธันวาคม ---
            "King Bhumibol's Birthday": "วันพ่อแห่งชาติ",
            "King Bhumibol's Birthday Observed": "ชดเชยวันพ่อแห่งชาติ",
            "King Bhumibol's Birthday/Father's Day": "วันพ่อแห่งชาติ",
            "King Bhumibol's Birthday/Father's Day Observed": "ชดเชยวันพ่อแห่งชาติ",
            "H.M. King Bhumibol Adulyadej's Birthday": "วันพ่อแห่งชาติ",
            "Constitution Day": "วันรัฐธรรมนูญ",
            "New Year's Eve": "วันสิ้นปี"
        };

        if (!apiKey) {
            console.error("❌ Missing CALENDARIFIC_API_KEY");
            return res.status(500).json({ success: false, message: "Server configuration error" });
        }
        
        const url = `https://calendarific.com/api/v2/holidays?&api_key=${apiKey}&country=TH&year=${year}&type=national`;
        
        console.log(`\n--- [Calendarific API] Start ---`);
        console.log(`📅 Year: ${year}, Month: ${month || 'All'}`);
        
        const response = await axios.get(url);
        let holidays = [];

        if (response.data && response.data.response && response.data.response.holidays) {
            const rawHolidays = response.data.response.holidays;
            
            // ✅ Log ดูชื่อภาษาอังกฤษที่ API ส่งมาจริงๆ
            console.log(`📦 Raw names from API:`, rawHolidays.map(h => h.name));

            holidays = rawHolidays.map(h => {
                const englishName = h.name;
                const thaiName = holidayThaiNames[englishName] || englishName;
                
                // ✅ Log ตรวจสอบการ Mapping ทีละวัน
                if (thaiName !== englishName) {
                    console.log(`✅ Mapped: "${englishName}" -> "${thaiName}"`);
                } else {
                    console.log(`⚠️ Not Mapped: "${englishName}" (No Thai name found)`);
                }

                return {
                    HolidayDate: h.date.iso, 
                    HolidayName: thaiName 
                };
            });

            if (month) {
                const targetMonth = parseInt(month);
                holidays = holidays.filter(h => {
                    const m = parseInt(h.HolidayDate.split('-')[1]);
                    return m === targetMonth;
                });
            }
        }

        console.log(`🚀 Final Output count: ${holidays.length}`);
        console.log(`--- [Calendarific API] End ---\n`);

        res.json({ success: true, holidays });

    } catch (err) {
        console.error("❌ Calendarific Error:", err.message);
        res.json({ success: false, holidays: [] });
    }
});
server.listen(port, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${port}`);
});
