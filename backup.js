const mysqldump = require('mysqldump');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

const dbConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT || 4000,
    ssl: {
        minVersion: 'TLSv1.2',
        rejectUnauthorized: true
    }
};

async function runBackup() {
    const dateStr = new Date().toISOString().split('T')[0]; // เปลี่ยนชื่อให้ตรงกัน
    const fileName = `backup-${dateStr}.sql`;
    const filePath = path.join('/tmp', fileName);
    const backupDir = '/tmp/git_backup'; // สร้างโฟลเดอร์แยกทำงาน

    const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

    try {
        console.log('📦 กำลังดึงข้อมูลจาก Database...');
        await mysqldump({
            connection: dbConfig,
            dumpToFile: filePath,
        });

        console.log('🚀 กำลังเตรียมอัปโหลดไปที่ GitHub...');
        
        // ใช้โครงสร้างแบบสร้างโฟลเดอร์ใหม่เพื่อความชัวร์ 100%
        const gitCommands = `
            rm -rf ${backupDir} && 
            mkdir -p ${backupDir} && 
            cp ${filePath} ${backupDir}/${fileName} && 
            cd ${backupDir} && 
            git init && 
            git config user.name "Auto Backup" && 
            git config user.email "backup@autonurseshift.com" && 
            git remote add origin https://${GITHUB_TOKEN}@github.com/dunkamoljoajit/autonurseshift-backup.git && 
            git branch -M main && 
            git add . && 
            git commit -m "Auto-backup: ${dateStr}" && 
            git push -f origin main
        `;

        exec(gitCommands, (error, stdout, stderr) => {
            if (error) {
                console.error(`❌ Git Error: ${error.message}`);
            } else {
                console.log(`✅ สำรองข้อมูลเรียบร้อย: ${new Date().toLocaleString()}`);
            }
            
            // ล้างข้อมูลหลังจบงาน
            if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
            exec(`rm -rf ${backupDir}`);
        });

    } catch (err) {
        console.error('❌ เกิดข้อผิดพลาด:', err);
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
        exec(`rm -rf ${backupDir}`);
    }
}

runBackup();