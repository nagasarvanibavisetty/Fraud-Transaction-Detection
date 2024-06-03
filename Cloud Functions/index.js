const functions = require('@google-cloud/functions-framework');
const nodemailer = require('nodemailer');

// Email configuration setup
const transporter = nodemailer.createTransport({
  host: 'smtp.mailgun.org', // SMTP Host
  port: 587,               // SMTP Port (commonly 587 for TLS)         
  auth: {
    user: 'postmaster@sandboxc5905e88c02a4f6a8b68124174302d29.mailgun.org', // Your email
    pass: '338cea33470b35e2f02b7c2d9454cb55-19806d14-0c59d96b'          // Your email password or app-specific password
  },
});

// Register a CloudEvent callback with the Functions Framework that will
// be executed when the Pub/Sub trigger topic receives a message.
functions.cloudEvent('helloPubSub', async cloudEvent => {
  // Decode the Pub/Sub message from base64
  const base64data = cloudEvent.data.message.data;
  const messageData = base64data ? Buffer.from(base64data, 'base64').toString() : 'No data received';

  // Setup email options
  const mailOptions = {
    from: 'postmaster@sandboxc5905e88c02a4f6a8b68124174302d29.mailgun.org',    // sender address
    to: 'abhilashpatade09@gmail.com', // list of receivers
    subject: 'Fraud Alert',            // Subject line
    text: messageData,                 // plain text body
  };

  // Send the email
  try {
    let info = await transporter.sendMail(mailOptions);
    console.log('Email sent: %s', info.messageId);
  } catch (error) {
    console.error('Failed to send email:', error);
  }
});
